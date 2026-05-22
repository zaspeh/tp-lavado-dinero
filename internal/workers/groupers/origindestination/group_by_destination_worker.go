package origindestination

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type GroupByDestinationWorker struct {
	inputExchange     *middleware.ExchangeMiddleware
	outputQueue       middleware.Middleware
	destinationsStore *AccountStore
	maxBatchWeight    int
}

type GroupByDestinationWorkerConfig struct {
	ID                string
	MomHost           string
	MomPort           int
	InputExchangeName string
	OutputQueueName   string
	MaxBatchWeight    int
}
