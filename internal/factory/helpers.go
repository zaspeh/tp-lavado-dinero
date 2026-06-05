package factory

import (
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/periodfilter"
)

const (
	defaultEnvString = ""
	defaultEnvInt    = 0
)

func getEnvStrict(key string) (string, error) {
	value, ok := os.LookupEnv(key)
	if !ok {
		return defaultEnvString, NewEnvNotFoundError(key)
	}
	return value, nil
}

func getEnvIntStrict(key string) (int, error) {
	valStr, err := getEnvStrict(key)
	if err != nil {
		return defaultEnvInt, err
	}

	val, err := strconv.Atoi(valStr)
	if err != nil {
		return defaultEnvInt, NewEnvNotNumericError(key)
	}
	return val, nil
}

func buildPeriodFromEnv(startKey, endKey string) (periodfilter.Period, error) {
	startStr, err := getEnvStrict(startKey)
	if err != nil {
		return periodfilter.Period{}, err
	}

	endStr, err := getEnvStrict(endKey)
	if err != nil {
		return periodfilter.Period{}, err
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return periodfilter.Period{}, err
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return periodfilter.Period{}, err
	}

	return periodfilter.Period{
		StartDate: startTime,
		EndDate:   endTime,
	}, nil
}

func getEnvFloatStrict(key string) (float64, error) {
	valStr, err := getEnvStrict(key)
	if err != nil {
		return 0, err
	}

	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0, NewEnvNotNumericError(key)
	}

	return val, nil
}

func getEnvStringSliceStrict(key string) ([]string, error) {
	valStr, err := getEnvStrict(key)
	if err != nil {
		return nil, err
	}

	return strings.Split(valStr, ","), nil
}

func getCoordinationInformationFromEnv() (int, int, string, error) {
	workerID, err := getEnvIntStrict("ID")
	if err != nil {
		return 0, 0, "", err
	}

	workerCount, err := getEnvIntStrict("WORKER_COUNT")
	if err != nil {
		return 0, 0, "", err
	}

	workerExchangeName, err := getEnvStrict("WORKER_EXCHANGE_NAME")
	if err != nil {
		return 0, 0, "", err
	}

	return workerID, workerCount, workerExchangeName, nil
}

func getMomConfigFromEnv() (m.ConnSettings, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return m.ConnSettings{}, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return m.ConnSettings{}, err
	}

	return m.ConnSettings{
		Hostname: host,
		Port:     port,
	}, nil
}

func createInputOutputQueues() (m.Middleware, m.Middleware, error) {
	momConfig, err := getMomConfigFromEnv()
	if err != nil {
		return nil, nil, err
	}

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, nil, err
	}

	inputQueue, err := middleware.CreateQueueMiddleware(inputQueueName, momConfig)
	if err != nil {
		return nil, nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(outputQueueName, momConfig)
	if err != nil {
		inputQueue.Close()
		return nil, nil, err
	}

	return inputQueue, outputQueue, nil
}

func createInputExchangeOutputQueue() (m.Middleware, m.Middleware, error) {
	momConfig, err := getMomConfigFromEnv()
	if err != nil {
		return nil, nil, err
	}

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, nil, err
	}

	inputKeys := []string{inputExchangeName + "." + id}

	inputExchange, err := middleware.CreateExchangeMiddleware(inputExchangeName, inputKeys, momConfig)
	if err != nil {
		return nil, nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(outputQueueName, momConfig)
	if err != nil {
		inputExchange.Close()
		return nil, nil, err
	}

	return inputExchange, outputQueue, nil
}

func getCoordinator() (*c.EOFCoordinator, error) {
	workerID, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	momConfig, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: workerExchangeName,
		ConnSettings:      momConfig,
		WorkerID:          workerID,
		WorkerCount:       workerCount,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		return nil, err
	}

	return coordinator, nil
}
