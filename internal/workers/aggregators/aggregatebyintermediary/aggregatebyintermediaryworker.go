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
