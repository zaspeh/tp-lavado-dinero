package routers

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type IntermediaryRouter struct {
	inputQueue                          middleware.Middleware
	aggregateByIntermediaryExchange     *middleware.ExchangeMiddleware
	aggregateByIntermediaryWorkers      int
	aggregateByIntermediaryExchangeKeys []string

	groupByDestinationExchange     *middleware.ExchangeMiddleware
	groupByDestinationWorkers      int
	groupByDestinationExchangeKeys []string
}

type IntermediaryRouterConfig struct {
	InputQueueName                string
	AggregateByIntermediaryName   string
	AggregateByIntermediaryAmount int
	MomHost                       string
	MomPort                       int
}

func NewIntermediaryRouter(config IntermediaryRouterConfig) (*IntermediaryRouter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryKeys := make([]string, config.AggregateByIntermediaryAmount)
	for i := range aggregateByIntermediaryKeys {
		aggregateByIntermediaryKeys[i] = fmt.Sprintf("%s.%d", config.AggregateByIntermediaryName, i)
	}

	aggregateByIntermediaryExchange, err := middleware.CreateExchangeMiddleware(config.AggregateByIntermediaryName, aggregateByIntermediaryKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &IntermediaryRouter{
		inputQueue:                          inputQueue,
		aggregateByIntermediaryExchange:     aggregateByIntermediaryExchange,
		aggregateByIntermediaryWorkers:      config.AggregateByIntermediaryAmount,
		aggregateByIntermediaryExchangeKeys: aggregateByIntermediaryKeys,
	}, nil
}
