package factory

import (
	"fmt"
	"os"
	"strconv"
)

const (
	defaultEnvString = ""
	defaultEnvInt    = 0
)

type EnvError struct {
	Key     string
	Message string
}

func (e *EnvError) Error() string {
	return fmt.Sprintf("%s: %s", e.Message, e.Key)
}

func NewEnvNotFoundError(key string) error {
	return &EnvError{Key: key, Message: "Env var not found"}
}

func NewEnvNotNumericError(key string) error {
	return &EnvError{Key: key, Message: "Env var is not a valid integer"}
}

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
