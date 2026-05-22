package clientconnection

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/messagehandler"
)

const (
	// TODO CAMBIAR A ENV VAR DESPUES
	eofAmountExpected = 2
)

type ClientConnectionConfig struct {
	ID                      string
	Protocol                *external.ExternalProtocol
	MOMHostName             string
	MOMPort                 int
	CurrencyFilterQueueName string
	RawDataQueueName        string
	ClientExchangeName      string
}

type ClientConnection struct {
	id                  string
	protocol            *external.ExternalProtocol
	currencyFilterQueue m.Middleware
	rawDataQueue        m.Middleware
	resultExchange      *m.ExchangeMiddleware
	EOFamountReceived   int
	transactionCounter  int
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

	// TODO: descomentar cuando el sistema sea multicliente
	// exchangeRoutingKey := []string{clientExchangeName + "." + id}
	exchangeRoutingKey := []string{config.ClientExchangeName}
	resultExchange, err := m.CreateExchangeMiddleware(config.ClientExchangeName, exchangeRoutingKey, connSettings)
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

	return &ClientConnection{
		id:                  config.ID,
		protocol:            config.Protocol,
		currencyFilterQueue: currencyFilterQueue,
		resultExchange:      resultExchange,
		rawDataQueue:        rawDataQueue,
		transactionCounter:  0,
	}, nil
}

func (cc *ClientConnection) Run() error {
	go cc.resultExchange.StartConsuming(func(msg m.Message, ack, nack func()) {
		cc.handleResult(msg, ack, nack)
	})

	for {
		message, err := cc.protocol.ReceiveMsg()
		if err != nil {
			return err
		}
		if err = message.Handle(cc); err != nil {
			// TODO: podria ser un NACK envez de cerrar comunicacion.
			return err
		}
	}
}

func (cc *ClientConnection) HandleTransaction(msg request.Transaction) error {
	wrappedMessage, err := messagehandler.TransactionToProto(cc.id, msg)
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	wrappedMessage, err = messagehandler.TransactionToConvertionTransaction(cc.id, msg)
	if err != nil {
		return err
	}

	if err := cc.rawDataQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	cc.transactionCounter++

	return cc.protocol.SendAck()
}

func (cc *ClientConnection) HandleEOF(msg request.EOF) error {
	slog.Info("Received EOF from client", "clientID", cc.id)

	wrappedMessage, err := messagehandler.EOFToProto(
		cc.id,
		cc.transactionCounter,
	)
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	if err := cc.rawDataQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	return cc.protocol.SendAck()
}

func (cc *ClientConnection) handleResult(msg m.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_EOF_:
		cc.handleEOFFromWorker(ack, nack)
	case protobuf.MessageType_MICROTRANSACTION_RESULT:
		cc.handleMicrotransactionResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_MAXBANK_RESULT:
		cc.handleMaxBankResult(moneyLaundry, ack, nack)
	default:
		slog.Warn("received message with unknown type", "type", moneyLaundry.GetType())
		nack()
	}
}

func (cc *ClientConnection) handleEOFFromWorker(ack, nack func()) {
	cc.EOFamountReceived++
	slog.Info(
		"received EOF from worker",
		"count", cc.EOFamountReceived,
	)
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

func (cc *ClientConnection) Close() error {
	err := cc.currencyFilterQueue.Close()
	if err != nil {
		return err
	}
	err = cc.resultExchange.Close()
	if err != nil {
		return err
	}
	return cc.protocol.Close()
}
