package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/groupers/maxbank"
)

func buildMaxBankWorker() (workers.Worker, error) {
	inputExchangeName, err := getEnvStrict("INPUT_QUEUE_NAME")
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

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
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

	config := maxbank.MaxBankWorkerConfig{
		ID:                id,
		MomHost:           host,
		MomPort:           port,
		InputExchangeName: inputExchangeName,
		OutputQueueName:   outputQueueName,
		MaxBatchWeight:    maxBatchWeight,
	}

	return maxbank.NewMaxBankWorker(config)
}
