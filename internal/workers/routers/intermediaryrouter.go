package routers

import (
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
