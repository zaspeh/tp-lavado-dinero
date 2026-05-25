package filters

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type CurrencyFilter struct {
	inputQueue                  middleware.Middleware
	microtransactionFilterQueue middleware.Middleware
	bankRouterQueue             middleware.Middleware
	periodFilterQueue           middleware.Middleware
	currencyToFilter            string
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

	return &CurrencyFilter{
		inputQueue:                  inputQueue,
		microtransactionFilterQueue: microtransactionFilterQueue,
		bankRouterQueue:             bankRouterQueue,
		periodFilterQueue:           periodFilterQueue,
		currencyToFilter:            config.CurrencyToFilter,
	}, nil
}

func (f *CurrencyFilter) Run() error {
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
			f.broadcastEOFMessage(msg, ack, nack)
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
	for _, transaction := range transactions {
		if transaction.GetPaymentCurrency() == f.currencyToFilter {
			err := f.broadcastToQueues(clientID, transaction)
			if err != nil {
				nack()
				return
			}
		}
	}
	ack()
}

func (f *CurrencyFilter) broadcastToQueues(clientID string, transaction *protobuf.Transaction) error {
	//q1
	microtransaction := &protobuf.Microtransaction{
		Account:    transaction.GetAccount(),
		ToAccount:  transaction.GetToAccount(),
		AmountPaid: transaction.GetAmountPaid(),
	}

	serializedMessage, err := serializer.SerializeProtoMessageWithClientID(
		clientID,
		microtransaction,
		protobuf.MessageType_MICROTRANSACTION,
	)

	if err != nil {
		return err
	}

	if err := f.microtransactionFilterQueue.Send(*serializedMessage); err != nil {
		return err
	}

	//q2
	frombank := transaction.GetFromBank()
	transferSummary := &protobuf.TransferSummary{
		Account: transaction.Account,
		Amount:  transaction.GetAmountPaid(),
	}

	maxbank := &protobuf.MaxBank{
		FromBank: frombank,
		Payload: &protobuf.MaxBank_TransferSummary{
			TransferSummary: transferSummary,
		},
	}

	serializedMaxBankMessage, err := serializer.SerializeProtoMessage(maxbank, protobuf.MessageType_MAXBANK)
	if err != nil {
		return err
	}

	if err := f.bankRouterQueue.Send(*serializedMaxBankMessage); err != nil {
		return err
	}
	//q3

	periodFilter := &protobuf.PeriodFilter{
		Timestamp:     transaction.GetTimestamp(),
		FromBank:      transaction.GetFromBank(),
		ToBank:        transaction.GetToBank(),
		Account:       transaction.GetAccount(),
		ToAccount:     transaction.GetToAccount(),
		AmountPaid:    transaction.GetAmountPaid(),
		PaymentFormat: transaction.GetPaymentFormat(),
	}

	serializedPeriodFilter, err := serializer.SerializeProtoMessageWithClientID(clientID, periodFilter, protobuf.MessageType_PERIODFILTER)
	if err != nil {
		return err
	}

	if err := f.periodFilterQueue.Send(*serializedPeriodFilter); err != nil {
		return err
	}

	return nil
}

func (f *CurrencyFilter) broadcastEOFMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, _ := serializer.DeserializeMoneyLaundering(msg)
	slog.Info(
		"broadcasting EOF",
		"clientID",
		moneyLaundry.GetClientID(),
	)
	if err := f.microtransactionFilterQueue.Send(msg); err != nil {
		nack()
		return
	}

	if err := f.bankRouterQueue.Send(msg); err != nil {
		nack()
		return
	}

	if err := f.periodFilterQueue.Send(msg); err != nil {
		nack()
		return
	}

	ack()
}
