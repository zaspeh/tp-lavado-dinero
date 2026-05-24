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

func NewScatterGatherJoinWorker(config ScatterGatherJoinConfig) (*ScatterGatherJoinWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueueName,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(
		config.ClientExchangeName,
		[]string{config.ClientExchangeName},
		connSettings,
	)

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &ScatterGatherJoinWorker{
		inputQueue:         inputQueue,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
		store:              NewScatterGatherStore(),
		maxBatchWeight:     config.MaxBatchWeight,
	}, nil
}
