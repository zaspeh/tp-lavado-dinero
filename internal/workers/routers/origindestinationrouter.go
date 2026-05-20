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

func NewOriginDestinationRouter(config OriginDestinationRouterConfig, connSettings middleware.ConnSettings) (*OriginDestinationRouter, error) {
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	groupByOriginQueue, err := middleware.CreateQueueMiddleware(config.GroupByOriginQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	groupByDestinationQueue, err := middleware.CreateQueueMiddleware(config.GroupByDestinationQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	return &OriginDestinationRouter{
		InputQueue:                   inputQueue,
		groupByOriginQueue:           groupByOriginQueue,
		groupByDestinationQueue:      groupByDestinationQueue,
		maxGroupByOriginWorkers:      config.MaxGroupByOriginWorkers,
		maxGroupByDestinationWorkers: config.MaxGroupByDestinationWorkers,
	}, nil
}
