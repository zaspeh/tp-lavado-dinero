package factory

import "fmt"

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
