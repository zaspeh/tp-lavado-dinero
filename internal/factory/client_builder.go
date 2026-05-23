package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/client"
)

func buildClient() (*client.Client, error) {
	serverHost, err := getEnvStrict("SERVER_HOST")
	if err != nil {
		return nil, err
	}

	serverPort, err := getEnvStrict("SERVER_PORT")
	if err != nil {
		return nil, err
	}

	inputFile, err := getEnvStrict("INPUT_FILE_TRANSACTIONS")
	if err != nil {
		return nil, err
	}

	outputDir, err := getEnvStrict("OUTPUT_DIR")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	config := client.ClientConfig{
		ServerHost:     serverHost,
		ServerPort:     serverPort,
		InputFile:      inputFile,
		OutputDir:      outputDir,
		MaxBatchWeight: maxBatchWeight,
	}

	return client.New(config)
}
