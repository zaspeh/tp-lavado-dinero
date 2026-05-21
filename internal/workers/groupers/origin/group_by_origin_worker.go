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

func NewGroupByOriginWorker(config GroupByOriginWorkerConfig) (*GroupByOriginWorker, error) {
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

	return &GroupByOriginWorker{
		inputExchange:  inputExchange,
		outputQueue:    outputQueue,
		originsStore:   newAccountStore(),
		maxBatchWeight: config.MaxBatchWeight,
	}, nil
}
