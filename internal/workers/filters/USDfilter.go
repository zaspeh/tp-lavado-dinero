package filters

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type USDFilter struct {
	usdQueue          middleware.Middleware
	amountFilterQueue middleware.Middleware
	bankRouterQueue   middleware.Middleware
	periodFilterQueue middleware.Middleware
}

type USDFilterConfig struct {
	USDQueueName                    string
	MicrotransactionFilterQueueName string
	BankRouterQueueName             string
	PeriodFilterQueueName           string
	MomHost                         string
	MomPort                         int
}

func NewUSDFilter(config USDFilterConfig) (*USDFilter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	usdQueue, err := middleware.CreateQueueMiddleware(config.USDQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	amountFilterQueue, err := middleware.CreateQueueMiddleware(config.MicrotransactionFilterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		return nil, err
	}

	bankRouterQueue, err := middleware.CreateQueueMiddleware(config.BankRouterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		amountFilterQueue.Close()
		return nil, err
	}

	periodFilterQueue, err := middleware.CreateQueueMiddleware(config.PeriodFilterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		amountFilterQueue.Close()
		bankRouterQueue.Close()
		return nil, err
	}

	return &USDFilter{
		usdQueue:          usdQueue,
		amountFilterQueue: amountFilterQueue,
		bankRouterQueue:   bankRouterQueue,
		periodFilterQueue: periodFilterQueue,
	}, nil
}

func (f *USDFilter) Run() {
	f.usdQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		// Recibo un mensaje y filtro que sea Dolar, entonces envío a todas las colas.
		// Si no es Dolar, hago ACK y no envío a ninguna cola.
		// En caso de error, hago NACK para que el mensaje vuelva a la cola.
		moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
		if err != nil {
			nack()
			return
		}

		switch moneyLaundry.Type {
		case protobuf.MessageType_TRANSACTION:
			f.handleTransactionMessage(moneyLaundry, ack, nack)
		default:
			ack()
		}
	})
}

func (f *USDFilter) handleTransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	transaction, err := serializer.DeserializeTransaction(moneyLaundry.Payload, &protobuf.Transaction{})
	if err != nil {
		nack()
		return
	}

	if transaction.PaymentCurrency == "USD" {
		f.broadcastToQueues(transaction, ack, nack)
	}

	ack()
}

func (f *USDFilter) broadcastToQueues(transaction *protobuf.Transaction, ack, nack func()) {
	//q1
	microtransaction := &protobuf.Microtransaction{
		FromBank:   transaction.FromBank,
		ToBank:     transaction.ToBank,
		Account:    transaction.Account,
		ToAccount:  transaction.ToAccount,
		AmountPaid: transaction.AmountPaid,
	}

	serializedMessage, err := serializer.SerializeProtoMessage(microtransaction, protobuf.MessageType_MICROTRANSACTION)

	if err != nil {
		nack()
		return
	}

	if err := f.amountFilterQueue.Send(*serializedMessage); err != nil {
		nack()
		return
	}

	//q2
}
