package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/mappers/currencyconverter"
)

func buildCurrencyConverterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	apiURL, err := getEnvStrict("EXCHANGE_RATE_API_URL")
	if err != nil {
		return nil, err
	}

	converter, err := currencyconverter.NewCurrencyConverter(apiURL)
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := currencyconverter.CurrencyConverterConfig{
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		MomHost:         host,
		MomPort:         port,
		Converter:       converter,
		WorkerID:        id,
		WorkerCount:     workerCount,
		WorkerExchange:  workerExchangeName,
	}

	return currencyconverter.NewCurrencyConverterWorker(config)
}
