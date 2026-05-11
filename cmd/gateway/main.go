package gateway

import (
	"errors"
	"log/slog"
	"os"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/gateway"
)

func loadConfig() (gateway.GatewayConfig, error) {
	USDQueueName := os.Getenv("USD_QUEUE")
	if USDQueueName == "" {
		return gateway.GatewayConfig{}, errors.New("USD_QUEUE is required")
	}

	outputQueueName := os.Getenv("OUTPUT_QUEUE")
	if outputQueueName == "" {
		return gateway.GatewayConfig{}, errors.New("OUTPUT_QUEUE is required")
	}

	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_HOST is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return gateway.GatewayConfig{}, errors.New("SERVER_PORT is required")
	}

	momPort, err := strconv.Atoi(os.Getenv("MOM_PORT"))
	if err != nil {
		return gateway.GatewayConfig{}, errors.New("MOM_PORT is required and must be a number")
	}

	momHost := os.Getenv("MOM_HOST")
	if momHost == "" {
		return gateway.GatewayConfig{}, errors.New("MOM_HOST is required")
	}

	return gateway.GatewayConfig{
		USDQueueName:    USDQueueName,
		OutputQueueName: outputQueueName,
		ServerHost:      serverHost,
		ServerPort:      serverPort,
		MomHost:         momHost,
		MomPort:         momPort,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error(
			"while loading config",
			"err", err,
		)
		return 1
	}

	server, err := gateway.New(config)
	if err != nil {
		slog.Error(
			"while creating gateway",
			"err", err,
		)
		return 1
	}

	if err := server.Run(); err != nil {
		slog.Error(
			"gateway stopped with error",
			"err", err,
		)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
