package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/routers"
)

func buildBankRouterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

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

	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := routers.BackRouterConfig{
		ID:                  id,
		MomHost:             host,
		MomPort:             port,
		InputQueueName:      inQ,
		MaxBankExchangeName: maxBankExchangeName,
		MaxBankWorkerAmount: maxBankWorkerAmount,
		WorkerCount:         workerCount,
		WorkerExchangeName:  workerExchangeName,
	}

	return routers.NewBankRouter(config)
}

func buildOriginDestinationRouterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}
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
	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()

	if err != nil {
		return nil, err
	}

	config := routers.OriginDestinationRouterConfig{
		ID:                              id,
		InputQueueName:                  inQ,
		GroupByOriginExchangeName:       groupByOriginExchangeName,
		GroupByDestinationExchangeName:  groupByDestinationExchangeName,
		GroupByOriginWorkersAmount:      groupByOriginWorkerAmount,
		GroupByDestinationWorkersAmount: groupByDestinationWorkerAmount,
		MomHost:                         host,
		MomPort:                         port,
		WorkerCount:                     workerCount,
		WorkerExchangeName:              workerExchangeName,
	}

	return routers.NewOriginDestinationRouter(config)
}

func buildPaymentTypeRouterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueue, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("PAYMENT_TYPE_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	avgByTypeWorkerAmount, err := getEnvIntStrict("AVG_BY_TYPE_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	config := routers.PaymentTypeRouterConfig{
		InputQueueName:          inputQueue,
		PaymentTypeExchangeName: exchangeName,
		AvgByTypeWorkerAmount:   avgByTypeWorkerAmount,
		MomHost:                 host,
		MomPort:                 port,
	}

	return routers.NewPaymentTypeRouter(config)
}

func buildIntermediaryRouterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueue, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("AGGREGATE_BY_INTERMEDIARY_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryWorkerAmount, err := getEnvIntStrict("AGGREGATE_BY_INTERMEDIARY_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}
	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()

	if err != nil {
		return nil, err
	}

	config := routers.IntermediaryRouterConfig{
		ID:                            id,
		InputQueueName:                inputQueue,
		AggregateByIntermediaryName:   exchangeName,
		AggregateByIntermediaryAmount: aggregateByIntermediaryWorkerAmount,
		MomHost:                       host,
		MomPort:                       port,
		WorkerCount:                   workerCount,
		WorkerExchangeName:            workerExchangeName,
	}

	return routers.NewIntermediaryRouter(config)

}
