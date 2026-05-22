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

func NewGroupByDestinationWorker(config GroupByDestinationWorkerConfig) (*GroupByDestinationWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputExchangeKeys := []string{config.InputExchangeName + "." + config.ID}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, inputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &GroupByDestinationWorker{
		inputExchange:     inputExchange,
		outputQueue:       outputQueue,
		destinationsStore: newAccountStore(),
		maxBatchWeight:    config.MaxBatchWeight,
	}, nil
}
