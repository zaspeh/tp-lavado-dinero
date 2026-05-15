package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/zaspeh/tp-lavado-dinero/internal/client"
)

func loadConfig() (client.ClientConfig, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.ClientConfig{}, errors.New("SERVER_HOST is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.ClientConfig{}, errors.New("SERVER_PORT is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.ClientConfig{}, errors.New("INPUT_FILE is required")
	}

	outputDir := os.Getenv("OUTPUT_DIR")
	if outputDir == "" {
		return client.ClientConfig{}, errors.New("OUTPUT_DIR is required")
	}

	return client.ClientConfig{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputFile:  inputFile,
		OutputDir:  outputDir,
	}, nil
}

func run() int {
	config, err := loadConfig()
	if err != nil {
		slog.Error("while loading config", "err", err)
		return 1
	}

	c, err := client.New(config)
	if err != nil {
		slog.Error("while creating client", "err", err)
		return 1
	}

	if err := c.Run(); err != nil {
		slog.Error("client stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
