package filters

import (
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
		moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
		if err != nil {
			nack()
			return
		}

		switch moneyLaundry.Type {
		case protobuf.MessageType_TRANSACTION:
			f.handleTransactionMessage(moneyLaundry, ack, nack)

		case protobuf.MessageType_EOF:
			f.broadcastEOFMessage(msg, ack, nack)
		default:
			nack()
		}
	})
	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (f *CurrencyFilter) handleTransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	transaction, err := serializer.DeserializeTransaction(moneyLaundry.Payload, &protobuf.Transaction{})
	if err != nil {
		nack()
		return
	}

	if transaction.GetPaymentCurrency() == f.currencyToFilter {
		err := f.broadcastToQueues(transaction)
		if err != nil {
			nack()
			return
		}
	}

	ack()
}

func (f *CurrencyFilter) broadcastToQueues(transaction *protobuf.Transaction) error {
	//q1
	microtransaction := &protobuf.Microtransaction{
		FromBank:   transaction.GetFromBank(),
		ToBank:     transaction.GetToBank(),
		Account:    transaction.GetAccount(),
		ToAccount:  transaction.GetToAccount(),
		AmountPaid: transaction.GetAmountPaid(),
	}

	serializedMessage, err := serializer.SerializeProtoMessage(microtransaction, protobuf.MessageType_MICROTRANSACTION)

	if err != nil {
		return err
	}

	if err := f.microtransactionFilterQueue.Send(*serializedMessage); err != nil {
		return err
	}

	//q2
	//TODO:, no deberia convertir a string
	frombank := string(transaction.GetFromBank())
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

	return nil

	//q3

}

func (f *CurrencyFilter) broadcastEOFMessage(msg middleware.Message, ack, nack func()) {
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
