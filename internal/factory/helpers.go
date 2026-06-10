package factory

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/heartbeat"
	filterprocessor "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/filters"
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

func buildPeriodFromEnv(startKey, endKey string) (filterprocessor.Period, error) {
	startStr, err := getEnvStrict(startKey)
	if err != nil {
		return filterprocessor.Period{}, err
	}

	endStr, err := getEnvStrict(endKey)
	if err != nil {
		return filterprocessor.Period{}, err
	}

	startTime, err := time.Parse(time.RFC3339, startStr)
	if err != nil {
		return filterprocessor.Period{}, err
	}

	endTime, err := time.Parse(time.RFC3339, endStr)
	if err != nil {
		return filterprocessor.Period{}, err
	}

	return filterprocessor.Period{
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

	queuesNames := []string{"INPUT_QUEUE_NAME", "OUTPUT_QUEUE_NAME"}
	queues, err := createQueues(queuesNames, momConfig)
	if err != nil {
		return nil, nil, err
	}

	return queues[0], queues[1], nil
}

func createInputExchangeOutputQueue(keyPrefix string) (m.Middleware, m.Middleware, error) {
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

	if keyPrefix == "" {
		keyPrefix = inputExchangeName
	}
	inputKeys := []string{keyPrefix + "." + id}

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

func createQueues(queuesAlias []string, momConfig m.ConnSettings) ([]m.Middleware, error) {
	queuesNames := make([]string, len(queuesAlias))
	for i, key := range queuesAlias {
		if name, err := getEnvStrict(key); err != nil {
			return nil, err
		} else {
			queuesNames[i] = name
		}
	}

	queues := make([]m.Middleware, len(queuesNames))
	for i, queueName := range queuesNames {
		queue, err := middleware.CreateQueueMiddleware(queueName, momConfig)
		if err != nil {
			for j := range i {
				queues[j].Close()
			}
			return nil, err
		}
		queues[i] = queue
	}

	return queues, nil
}

func closeQueues(queues []m.Middleware) {
	for _, q := range queues {
		q.Close()
	}
}

func createExchangeOutput(momConfig m.ConnSettings, exchangeNameKey string, workerAmountName string) (*middleware.ExchangeMiddleware, []string, error) {
	exchangeName, err := getEnvStrict(exchangeNameKey)
	if err != nil {
		return nil, nil, err
	}

	workerAmount, err := getEnvIntStrict(workerAmountName)
	if err != nil {
		return nil, nil, err
	}

	exchangeKeys := make([]string, workerAmount)

	for i := range exchangeKeys {
		exchangeKeys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	paymentTypeExchange, err := middleware.CreateExchangeMiddleware(exchangeName, exchangeKeys, momConfig)
	if err != nil {
		return nil, nil, err
	}

	return paymentTypeExchange, exchangeKeys, nil
}

func buildHeartbeatPublisher() (*heartbeat.HeartbeatPublisher, error) {
	momConfig, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	heartbeatQueueName, err := getEnvStrict("HEARTBEAT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	intervalSeconds, err := getEnvIntStrict("HEARTBEAT_INTERVAL_SECONDS")
	if err != nil {
		return nil, err
	}

	heartbeatQueue, err := middleware.CreateQueueMiddleware(heartbeatQueueName, momConfig)
	if err != nil {
		return nil, err
	}

	containerName, err := getEnvStrict("CONTAINER_NAME")
	if err != nil {
		return nil, err
	}

	return heartbeat.NewHeartbeatPublisher(heartbeatQueue, containerName, intervalSeconds), nil
}
