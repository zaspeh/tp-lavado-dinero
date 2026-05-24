package aggregatebyintermediary

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type AggregateByIntermediaryWorker struct {
	originInputExchange      *middleware.ExchangeMiddleware
	destinationInputExchange *middleware.ExchangeMiddleware

	outputQueue middleware.Middleware

	store *IntermediaryStore
}

type AggregateByIntermediaryWorkerConfig struct {
	ID                           string
	MomHost                      string
	MomPort                      int
	OriginInputExchangeName      string
	DestinationInputExchangeName string
	OutputQueueName              string
}

func NewAggregateByIntermediaryWorker(config AggregateByIntermediaryWorkerConfig) (*AggregateByIntermediaryWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	originInputExchangeKeys := []string{config.OriginInputExchangeName + "." + config.ID}
	originInputExchange, err := middleware.CreateExchangeMiddleware(config.OriginInputExchangeName, originInputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	destinationInputExchangeKeys := []string{config.DestinationInputExchangeName + "." + config.ID}
	destinationInputExchange, err := middleware.CreateExchangeMiddleware(config.DestinationInputExchangeName, destinationInputExchangeKeys, connSettings)
	if err != nil {
		originInputExchange.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		originInputExchange.Close()
		destinationInputExchange.Close()
		return nil, err
	}

	return &AggregateByIntermediaryWorker{
		originInputExchange:      originInputExchange,
		destinationInputExchange: destinationInputExchange,
		outputQueue:              outputQueue,
		store:                    NewIntermediaryStore(),
	}, nil
}
