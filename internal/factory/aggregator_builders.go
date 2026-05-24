package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/aggregators/aggregatebyintermediary"
)

func buildAggregateByIntermediaryWorker() (workers.Worker, error) {
	originInputExchangeName, err := getEnvStrict("ORIGIN_INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	destinationInputExchangeName, err := getEnvStrict("DESTINATION_INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
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

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := aggregatebyintermediary.AggregateByIntermediaryWorkerConfig{
		ID:                           id,
		MomHost:                      host,
		MomPort:                      port,
		OriginInputExchangeName:      originInputExchangeName,
		DestinationInputExchangeName: destinationInputExchangeName,
		OutputQueueName:              outputQueueName,
		MaxBatchWeight:               maxBatchWeight,
	}

	return aggregatebyintermediary.NewAggregateByIntermediaryWorker(config)
}
