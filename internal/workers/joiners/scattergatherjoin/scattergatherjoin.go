package scattergatherjoin

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type ScatterGatherJoinWorker struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string

	store          *ScatterGatherStore
	maxBatchWeight int
}

type ScatterGatherJoinConfig struct {
	InputQueueName string

	ClientExchangeName string

	MomHost        string
	MomPort        int
	MaxBatchWeight int
}
