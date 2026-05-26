package filters

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type CurrencyFilter struct {
	inputQueue                  middleware.Middleware
	microtransactionFilterQueue middleware.Middleware
	bankRouterQueue             middleware.Middleware
	periodFilterQueue           middleware.Middleware
	currencyToFilter            string
	coordinator                 *c.EOFCoordinator
	transactionBatchers         map[string]*batch.Batcher[*protobuf.Transaction, *protobuf.TransactionBatch]
}

type CurrencyFilterConfig struct {
	ID                              int
	WorkerCount                     int
	WorkerExchangeName              string
	InputQueueName                  string
	MicrotransactionFilterQueueName string
	BankRouterQueueName             string
	PeriodFilterQueueName           string
	MomHost                         string
	MomPort                         int
	CurrencyToFilter                string
}

func NewCurrencyFilter(config CurrencyFilterConfig) (*CurrencyFilter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	microtransactionFilterQueue, err := middleware.CreateQueueMiddleware(config.MicrotransactionFilterQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	bankRouterQueue, err := middleware.CreateQueueMiddleware(config.BankRouterQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		microtransactionFilterQueue.Close()
		return nil, err
	}

	periodFilterQueue, err := middleware.CreateQueueMiddleware(config.PeriodFilterQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		microtransactionFilterQueue.Close()
		bankRouterQueue.Close()
		return nil, err
	}

	currencyFilter := &CurrencyFilter{
		inputQueue:                  inputQueue,
		microtransactionFilterQueue: microtransactionFilterQueue,
		bankRouterQueue:             bankRouterQueue,
		periodFilterQueue:           periodFilterQueue,
		currencyToFilter:            config.CurrencyToFilter,
		transactionBatchers:         make(map[string]*batch.Batcher[*protobuf.Transaction, *protobuf.TransactionBatch]),
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.ID,
		WorkerCount:       config.WorkerCount,
		FlushHandler:      currencyFilter.broadcastEOFMessage,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		microtransactionFilterQueue.Close()
		bankRouterQueue.Close()
		periodFilterQueue.Close()
		return nil, err
	}

	currencyFilter.coordinator = coordinator
	return currencyFilter, nil
}

func (f *CurrencyFilter) Run() error {
	go f.coordinator.Run()
	f.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
		if err != nil {
			nack()
			return
		}

		switch moneyLaundry.Type {
		case protobuf.MessageType_TRANSACTION_BATCH:
			f.handleTransactionBatchMessage(moneyLaundry, ack, nack)
		case protobuf.MessageType_EOF_:
			f.handleEOFMessage(moneyLaundry, ack, nack)
		default:
			nack()
		}
	})
	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (f *CurrencyFilter) handleTransactionBatchMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	transactions := moneyLaundry.GetTransactions().GetTransactions()
	clientID := moneyLaundry.GetClientID()
	batcher := f.getTransactionBatcher(clientID)
	for _, transaction := range transactions {
		if transaction.GetPaymentCurrency() == f.currencyToFilter {
			if err := batcher.Add(transaction); err != nil {
				nack()
				return
			}
			f.coordinator.RecordSurvivor(clientID)
		}
		f.coordinator.RecordProcessed(clientID)
	}

	// flush por el momento, idealmente no flusheamos por batch sino
	// que armamos mas batches cuando este se llene pero genera que se
	// maneje el coordinador en todas las instancias
	if err := batcher.Flush(); err != nil {
		nack()
		return
	}
	ack()
}

func (f *CurrencyFilter) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	if batcher := f.transactionBatchers[clientID]; batcher != nil {
		if err := batcher.Flush(); err != nil {
			nack()
			return
		}
	}
	eofMessage := moneyLaundry.GetEofMessage()
	err := f.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions())
	if err != nil {
		nack()
		return
	}
	ack()
}

func (f *CurrencyFilter) getTransactionBatcher(clientID string) *batch.Batcher[*protobuf.Transaction, *protobuf.TransactionBatch] {
	if batcher, ok := f.transactionBatchers[clientID]; ok {
		return batcher
	}

	transactionBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		protowrappers.WrapTransactions,
	)

	onFlush := func(batch *protobuf.TransactionBatch) error {
		return f.broadcastTransactionBatch(clientID, batch)
	}

	batcher := batch.NewBatcher(transactionBatch, onFlush)
	f.transactionBatchers[clientID] = batcher
	return batcher
}

func (f *CurrencyFilter) broadcastTransactionBatch(clientID string, batch *protobuf.TransactionBatch) error {
	transactions := batch.GetTransactions()
	if len(transactions) == 0 {
		return nil
	}

	if err := f.sendToMicrotransactionsFilter(clientID, transactions); err != nil {
		return err
	}

	if err := f.sendToMaxBankRouter(clientID, transactions); err != nil {
		return err
	}

	if err := f.sendToPeriodFilters(clientID, transactions); err != nil {
		return err
	}

	return nil
}

func (f *CurrencyFilter) sendToMicrotransactionsFilter(clientID string, transactions []*protobuf.Transaction) error {
	microtransactions := make([]*protobuf.Microtransaction, 0, len(transactions))
	for _, transaction := range transactions {
		microtransaction := &protobuf.Microtransaction{
			Account:    transaction.GetAccount(),
			ToAccount:  transaction.GetToAccount(),
			AmountPaid: transaction.GetAmountPaid(),
		}
		microtransactions = append(microtransactions, microtransaction)
	}

	microtransactionBatch := protowrappers.WrapToMicrotrasactionBatch(microtransactions)
	innerMessage := &protobuf.MoneyLaundry_MicrotransactionsBatch{
		MicrotransactionsBatch: microtransactionBatch,
	}

	serializedMicrotransactionMessage, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MICROTRANSACTION_BATCH,
		innerMessage,
	)
	if err != nil {
		return err
	}

	return f.microtransactionFilterQueue.Send(serializedMicrotransactionMessage)
}

func (f *CurrencyFilter) sendToMaxBankRouter(clientID string, transactions []*protobuf.Transaction) error {
	maxBankMessages := make([]*protobuf.MaxBank, 0, len(transactions))
	for _, transaction := range transactions {
		transferSummary := &protobuf.TransferSummary{
			Account: transaction.GetAccount(),
			Amount:  transaction.GetAmountPaid(),
		}

		maxbank := &protobuf.MaxBank{
			FromBank: transaction.GetFromBank(),
			Payload: &protobuf.MaxBank_TransferSummary{
				TransferSummary: transferSummary,
			},
		}
		maxBankMessages = append(maxBankMessages, maxbank)
	}

	maxBankBatch := protowrappers.WrapMaxBank(maxBankMessages)
	innerMessage := &protobuf.MoneyLaundry_MaxBankBatch{
		MaxBankBatch: maxBankBatch,
	}

	serializedMaxBankMessage, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MAXBANK_BATCH,
		innerMessage,
	)
	if err != nil {
		return err
	}

	return f.bankRouterQueue.Send(serializedMaxBankMessage)
}

func (f *CurrencyFilter) sendToPeriodFilters(clientID string, transactions []*protobuf.Transaction) error {
	for _, transaction := range transactions {
		periodFilter := &protobuf.PeriodFilter{
			Timestamp:     transaction.GetTimestamp(),
			FromBank:      transaction.GetFromBank(),
			ToBank:        transaction.GetToBank(),
			Account:       transaction.GetAccount(),
			ToAccount:     transaction.GetToAccount(),
			AmountPaid:    transaction.GetAmountPaid(),
			PaymentFormat: transaction.GetPaymentFormat(),
		}

		serializedPeriodFilter, err := serializer.SerializeProtoMessageWithClientID(
			clientID,
			periodFilter,
			protobuf.MessageType_PERIODFILTER,
		)
		if err != nil {
			return err
		}

		if err := f.periodFilterQueue.Send(*serializedPeriodFilter); err != nil {
			return err
		}
	}

	return nil
}

// Funcion a llamar cuando el coordinador indique que ya se recibieron
// todos los mensajes EOF de los nodos hermanos, para que se haga el flush
func (f *CurrencyFilter) broadcastEOFMessage(clientID string, newEOFCount uint64) error {
	if !f.coordinator.IsLeader() {
		return nil
	}

	slog.Info("Broadcasting EOF message", "clientID", clientID, "newEOFCount", newEOFCount)
	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: newEOFCount,
		},
	}

	serializedEOFMessage, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMessage)
	if err != nil {
		return err
	}

	if err := f.microtransactionFilterQueue.Send(serializedEOFMessage); err != nil {
		return err
	}

	if err := f.bankRouterQueue.Send(serializedEOFMessage); err != nil {
		return err
	}

	if err := f.periodFilterQueue.Send(serializedEOFMessage); err != nil {
		return err
	}

	return nil
}
