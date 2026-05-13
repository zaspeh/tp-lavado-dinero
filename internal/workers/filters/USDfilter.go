package filters

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type USDFilter struct {
	usdQueue          middleware.Middleware
	routerQueue       middleware.Middleware
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

	routerQueue, err := middleware.CreateQueueMiddleware(config.MicrotransactionFilterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		return nil, err
	}

	BankRouterQueue, err := middleware.CreateQueueMiddleware(config.BankRouterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		routerQueue.Close()
		return nil, err
	}

	PeriodFilterQueue, err := middleware.CreateQueueMiddleware(config.PeriodFilterQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		routerQueue.Close()
		BankRouterQueue.Close()
		return nil, err
	}

	return &USDFilter{
		usdQueue:          usdQueue,
		routerQueue:       routerQueue,
		bankRouterQueue:   BankRouterQueue,
		periodFilterQueue: PeriodFilterQueue,
	}, nil
}

/*
func (f *USDFilter) Run() {
	f.usdQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		// Recibo un mensaje y filtro que sea Dolar, entonces envío a todas las colas.
		// Si no es Dolar, hago ACK y no envío a ninguna cola.
		// En caso de error, hago NACK para que el mensaje vuelva a la cola.
		moneyLaundry = serializer.DeserializeMoneyLaundry(msg.Body)
		if moneyLaundry.Currency == "USD" {
			MicrotransactionMessage = serializer.SerializeMicrotransaction(moneyLaundry)

			// serializo el mensaje para MicrotransactionFilter
			// envío el mensaje a la cola de MicrotransactionFilter
		}
		ack()
	})
}
*/
