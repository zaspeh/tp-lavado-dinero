package routers

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type OriginDestinationRouter struct {
	InputQueue                   middleware.Middleware
	groupByOriginQueue           middleware.Middleware
	groupByDestinationQueue      middleware.Middleware
	maxGroupByOriginWorkers      int
	maxGroupByDestinationWorkers int
}

type OriginDestinationRouterConfig struct {
	InputQueueName               string
	GroupByOriginQueueName       string
	GroupByDestinationQueueName  string
	MaxGroupByOriginWorkers      int
	MaxGroupByDestinationWorkers int
}
