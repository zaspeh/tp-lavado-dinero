package origin

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type GroupByOriginWorker struct {
	inputExchange  *middleware.ExchangeMiddleware
	outputQueue    middleware.Middleware
	originsStore   *AccountStore
	maxBatchWeight int
}

type GroupByOriginWorkerConfig struct {
	ID                string
	MomHost           string
	MomPort           int
	InputExchangeName string
	OutputQueueName   string
	MaxBatchWeight    int
}
