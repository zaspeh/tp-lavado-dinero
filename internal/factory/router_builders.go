package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/routers"
)

func buildBankRouterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	maxBankExchangeName, err := getEnvStrict("MAX_BANK_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	maxBankWorkerAmount, err := getEnvIntStrict("MAX_BANK_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	config := routers.BackRouterConfig{
		MomHost:             host,
		MomPort:             port,
		InputQueueName:      inQ,
		MaxBankExchangeName: maxBankExchangeName,
		MaxBankWorkerAmount: maxBankWorkerAmount,
	}

	return routers.NewBankRouter(config)
}

func buildOriginDestinationRouterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	groupByOriginExchangeName, err := getEnvStrict("GROUP_BY_ORIGIN_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	groupByOriginWorkerAmount, err := getEnvIntStrict("GROUP_BY_ORIGIN_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	groupByDestinationExchangeName, err := getEnvStrict("GROUP_BY_DESTINATION_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	groupByDestinationWorkerAmount, err := getEnvIntStrict("GROUP_BY_DESTINATION_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	config := routers.OriginDestinationRouterConfig{
		InputQueueName:                  inQ,
		GroupByOriginExchangeName:       groupByOriginExchangeName,
		GroupByDestinationExchangeName:  groupByDestinationExchangeName,
		GroupByOriginWorkersAmount:      groupByOriginWorkerAmount,
		GroupByDestinationWorkersAmount: groupByDestinationWorkerAmount,
		MomHost:                         host,
		MomPort:                         port,
	}

	return routers.NewOriginDestinationRouter(config)
}
