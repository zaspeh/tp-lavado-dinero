package factory

import (
	"os"
	"strconv"
	"strings"
	"time"

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

func getCoordinationInformationFromEnv() (int, string, error) {
	workerCount, err := getEnvIntStrict("WORKER_COUNT")
	if err != nil {
		return 0, "", err
	}

	workerExchangeName, err := getEnvStrict("WORKER_EXCHANGE_NAME")
	if err != nil {
		return 0, "", err
	}

	return workerCount, workerExchangeName, nil
}
