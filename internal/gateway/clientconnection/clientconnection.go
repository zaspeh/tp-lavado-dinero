package clientconnection

import (
	"errors"
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/messagehandler"
)

const (
	// TODO CAMBIAR A ENV VAR DESPUES
	eofAmountExpected = 5
)

type ClientConnectionConfig struct {
	ID                      string
	Protocol                *external.ExternalProtocol
	MOMHostName             string
	MOMPort                 int
	CurrencyFilterQueueName string
	RawDataQueueName        string
	ClientExchangeName      string
	MaxBatchWeight          int
	MaxBankRouterQueue      string
}

type ClientConnection struct {
	id                  string
	protocol            *external.ExternalProtocol
	currencyFilterQueue m.Middleware
	rawDataQueue        m.Middleware
	maxBankRouter       m.Middleware
	resultExchange      *m.ExchangeMiddleware
	EOFamountReceived   int
	transactionCounter  int
	accountsCounter     int
	MaxBatchWeight      int
	transactionBatcher  *batch.Batcher[*protobuf.Transaction, *protobuf.TransactionBatch]
	rawDataBatcher      *batch.Batcher[*protobuf.ToConvertTransaction, *protobuf.ToConvertTransactionBatch]
	accountBatcher      *batch.Batcher[*protobuf.MaxBank, *protobuf.MaxBankBatch]
}

func New(config ClientConnectionConfig) (*ClientConnection, error) {
	connSettings := m.ConnSettings{
		Hostname: config.MOMHostName,
		Port:     config.MOMPort,
	}

	currencyFilterQueue, err := m.CreateQueueMiddleware(config.CurrencyFilterQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	personalKey := config.ClientExchangeName + "." + config.ID
	exchangeRoutingKeys := []string{personalKey}
	resultExchange, err := m.CreateExchangeMiddleware(config.ClientExchangeName, exchangeRoutingKeys, connSettings)
	if err != nil {
		currencyFilterQueue.Close()
		return nil, err
	}

	rawDataQueue, err := m.CreateQueueMiddleware(config.RawDataQueueName, connSettings)
	if err != nil {
		currencyFilterQueue.Close()
		resultExchange.Close()
		return nil, err
	}

	maxBankRouterQueue, err := m.CreateQueueMiddleware(config.MaxBankRouterQueue, connSettings)
	if err != nil {
		currencyFilterQueue.Close()
		resultExchange.Close()
		rawDataQueue.Close()
		return nil, err
	}

	return &ClientConnection{
		id:                  config.ID,
		protocol:            config.Protocol,
		currencyFilterQueue: currencyFilterQueue,
		resultExchange:      resultExchange,
		rawDataQueue:        rawDataQueue,
		maxBankRouter:       maxBankRouterQueue,
		transactionCounter:  0,
	}, nil
}

// TODO: poner el BatchSize en variable de entorno
func (cc *ClientConnection) setUpBatchers() {
	transactionBatch := batch.New(
		1000000,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		protowrappers.WrapTransactions,
	)

	toConvertTransactionBatch := batch.New(
		1000000,
		protowrappers.ProtoSizer[*protobuf.ToConvertTransaction](),
		protowrappers.WrapToConvertTransactions,
	)

	accountBatch := batch.New(
		1000000,
		protowrappers.ProtoSizer[*protobuf.MaxBank](),
		protowrappers.WrapMaxBank,
	)

	// Set up despues de inicializacion para tener acceso a handlers
	cc.transactionBatcher = batch.NewBatcher(transactionBatch, cc.sendTransactionBatch)
	cc.rawDataBatcher = batch.NewBatcher(toConvertTransactionBatch, cc.sendToConvertTransactionBatch)
	cc.accountBatcher = batch.NewBatcher(accountBatch, cc.sendAccountBatch)

	// Seteamos el mismo batch id default, internamente es un uuid.
	cc.transactionBatcher.SetNewBatchId(batch.DefaultBatchId)
	cc.rawDataBatcher.SetNewBatchId(batch.DefaultBatchId)
	cc.accountBatcher.SetNewBatchId(batch.DefaultBatchId)
}

func (cc *ClientConnection) Run() error {
	cc.setUpBatchers()
	if err := cc.resultExchange.SetUp(); err != nil {
		return err
	}

	go cc.resultExchange.StartConsuming(func(msg m.Message, ack, nack func()) {
		cc.handleResult(msg, ack, nack)
	})

	for {
		message, err := cc.protocol.ReceiveMsg()
		if err != nil {
			return cc.handleDisconection(err)
		}
		if err = message.Handle(cc); err != nil {
			cc.protocol.SendNack()
		}
	}
}

func (cc *ClientConnection) handleDisconection(err error) error {
	if errors.Is(err, socket.ErrConnectionClosed) {
		slog.Info("client disconnected", "clientID", cc.id)
		// return cc.broadcastCleanup()
		return nil
	}
	return err
}

func (cc *ClientConnection) broadcastCleanup() error {
	cleanupMsg, err := protobuf.SerializeProtoMessageONTRIAL(cc.id, protobuf.MessageType_CLEANUP, nil, "")
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(cleanupMsg); err != nil {
		return err
	}

	if err := cc.resultExchange.Send(cleanupMsg); err != nil {
		return err
	}

	if err := cc.rawDataQueue.Send(cleanupMsg); err != nil {
		return err
	}

	if err := cc.maxBankRouter.Send(cleanupMsg); err != nil {
		return err
	}

	return nil
}

func (cc *ClientConnection) HandleTransactionBatch(msg request.TransactionBatch) error {
	slog.Debug("Received transaction batch from client", "clientID", cc.id, "batchSize", len(msg))
	for _, transaction := range msg {
		protoTransaction, err := messagehandler.RawTransactionToProtoTransaction(transaction)
		if err != nil {
			return err
		}

		if err := cc.transactionBatcher.Add(protoTransaction); err != nil {
			return err
		}

		protoToConvertTransaction := messagehandler.ProtoTransactionToProtoConvTransaction(protoTransaction)
		if err := cc.rawDataBatcher.Add(protoToConvertTransaction); err != nil {
			return err
		}

		cc.transactionCounter++
	}

	return cc.protocol.SendAck()
}

func (cc *ClientConnection) sendTransactionBatch(batch *protobuf.TransactionBatch, batchID string) error {
	innerMessage := &protobuf.MoneyLaundry_Transactions{
		Transactions: batch,
	}
	msg, err := protobuf.SerializeProtoMessageONTRIAL(cc.id, protobuf.MessageType_TRANSACTION_BATCH, innerMessage, batchID)
	if err != nil {
		return err
	}

	return cc.currencyFilterQueue.Send(msg)
}

func (cc *ClientConnection) sendToConvertTransactionBatch(batch *protobuf.ToConvertTransactionBatch, batchID string) error {
	innerMessage := &protobuf.MoneyLaundry_ToConvertBatch{
		ToConvertBatch: batch,
	}
	msg, err := protobuf.SerializeProtoMessageONTRIAL(cc.id, protobuf.MessageType_TO_CONVERT_TRANSACTION_BATCH, innerMessage, batchID)
	if err != nil {
		return err
	}

	return cc.rawDataQueue.Send(msg)
}

func (cc *ClientConnection) HandleAccountBatch(msg request.AccountBatch) error {
	slog.Debug("Received account batch from client", "clientID", cc.id, "batchSize", len(msg))
	for _, transaction := range msg {
		protoMaxBank, err := messagehandler.RawAccountToProtoMaxBank(transaction)
		if err != nil {
			return err
		}

		if err := cc.accountBatcher.Add(protoMaxBank); err != nil {
			return err
		}
		cc.accountsCounter++
	}

	return cc.protocol.SendAck()
}

func (cc *ClientConnection) sendAccountBatch(batch *protobuf.MaxBankBatch, batchID string) error {
	innerMessage := &protobuf.MoneyLaundry_MaxBankBatch{
		MaxBankBatch: batch,
	}
	msg, err := protobuf.SerializeProtoMessageONTRIAL(cc.id, protobuf.MessageType_MAXBANK_BATCH, innerMessage, batchID)
	if err != nil {
		return err
	}

	return cc.maxBankRouter.Send(msg)
}

func (cc *ClientConnection) HandleEOF(msg request.EOF) error {
	slog.Info("Received EOF from client", "clientID", cc.id, "totalTransactions", cc.transactionCounter)

	// Liberamos el batcher por si quedó algo pendiente
	if err := cc.transactionBatcher.Flush(); err != nil {
		return err
	}

	if err := cc.rawDataBatcher.Flush(); err != nil {
		return err
	}

	if err := cc.accountBatcher.Flush(); err != nil {
		return err
	}

	eofTransactions, err := messagehandler.EOFToProto(cc.id, cc.transactionCounter)
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(eofTransactions); err != nil {
		return err
	}

	if err := cc.rawDataQueue.Send(eofTransactions); err != nil {
		return err
	}

	eofAccounts, err := messagehandler.EOFToProto(cc.id, cc.accountsCounter)
	if err != nil {
		return err
	}

	if err := cc.maxBankRouter.Send(eofAccounts); err != nil {
		return err
	}

	return cc.protocol.SendAck()
}

func (cc *ClientConnection) handleResult(msg m.Message, ack, nack func()) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_EOF_:
		cc.handleEOFFromWorker(ack, nack)
	case protobuf.MessageType_MICROTRANSACTION_RESULT:
		cc.handleMicrotransactionResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_MAX_BANK_RESULT_BATCH:
		cc.handleMaxBankResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_CONVERTED_MICRO_PAYMENT_RESULT:
		cc.handleConvertedMicroPaymentResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_AVGBYTYPE_RESULT:
		cc.handleAvgByTypeResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_SUSPICIOUS_ACCOUNT_BATCH:
		cc.handleSuspiciousAccountBatch(moneyLaundry, ack, nack)
	default:
		slog.Warn("received message with unknown type", "type", moneyLaundry.GetType())
		nack()
	}
}

func (cc *ClientConnection) handleEOFFromWorker(ack, nack func()) {
	cc.EOFamountReceived++
	slog.Info("received EOF from worker")
	if cc.EOFamountReceived == eofAmountExpected {
		slog.Info("sending EOF to client")
		if err := cc.protocol.SendEOF(); err != nil {
			cc.EOFamountReceived--
			nack()
			return
		}
	}
	ack()
}

func (cc *ClientConnection) handleAvgByTypeResult(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	avgResults, err := messagehandler.ProtoToAvgByTypeResults(moneyLaundry)
	if err != nil {
		nack()
		return
	}

	for i := range avgResults {
		msgResult := &avgResults[i]
		if err := cc.protocol.SendAvgByTypeResult(msgResult); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (cc *ClientConnection) handleMicrotransactionResult(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	microtransactionResult, err := serializer.DeserializeTransaction(
		moneyLaundry.GetPayload(),
		&protobuf.MicrotransactionResult{},
	)

	if err != nil {
		nack()
		return
	}

	// TODO: usar message handler para convertir de proto a external
	msgResult := &result.MicrotransactionResult{
		Transactions: microtransactionResult.Transactions,
	}

	if err := cc.protocol.SendMicrotransactionResult(msgResult); err != nil {
		nack()
		return
	}

	ack()
}

func (cc *ClientConnection) handleMaxBankResult(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	externalMsg, err := messagehandler.ProtoToMaxBankResult(moneyLaundering)
	if err != nil {
		nack()
		return
	}

	if err := cc.protocol.SendMaxBankResult(externalMsg); err != nil {
		nack()
		return
	}
	ack()
}

func (cc *ClientConnection) handleConvertedMicroPaymentResult(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	externalMsg, err := messagehandler.ProtoToConvertedMicroPaymentResult(moneyLaundering)
	if err != nil {
		nack()
		return
	}

	if err := cc.protocol.SendConvertedMicroPaymentResult(externalMsg); err != nil {
		nack()
		return
	}
	ack()
}

func (cc *ClientConnection) handleSuspiciousAccountBatch(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {

	externalMsg, err := messagehandler.ProtoToSuspiciousAccounts(moneyLaundering)

	if err != nil {
		nack()
		return
	}

	if err := cc.protocol.SendSuspiciousAccountsResult(
		externalMsg,
	); err != nil {
		nack()
		return
	}

	ack()
}

func (cc *ClientConnection) Close() error {
	err := cc.currencyFilterQueue.Close()
	if err != nil {
		return err
	}
	err = cc.resultExchange.Close()
	if err != nil {
		return err
	}
	err = cc.rawDataQueue.Close()
	if err != nil {
		return err
	}
	err = cc.maxBankRouter.Close()
	if err != nil {
		return err
	}
	return cc.protocol.Close()
}
