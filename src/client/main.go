package main

import (
	"errors"
	"log/slog"
	"os"

	"github.com/google/uuid"

	"tp-lavado-dinero/client"
)

func loadConfig() (client.Config, error) {
	serverHost := os.Getenv("SERVER_HOST")
	if serverHost == "" {
		return client.Config{}, errors.New("SERVER_HOST is required")
	}

	serverPort := os.Getenv("SERVER_PORT")
	if serverPort == "" {
		return client.Config{}, errors.New("SERVER_PORT is required")
	}

	inputFile := os.Getenv("INPUT_FILE")
	if inputFile == "" {
		return client.Config{}, errors.New("INPUT_FILE is required")
	}

	outputDir := os.Getenv("OUTPUT_DIR")
	if outputDir == "" {
		return client.Config{}, errors.New("OUTPUT_DIR is required")
	}

	clientID := os.Getenv("CLIENT_ID")
	if clientID == "" {
		clientID = "client-default"
	}

	return client.Config{
		ServerHost: serverHost,
		ServerPort: serverPort,
		InputFile:  inputFile,
		OutputDir:  outputDir,
		ClientID:   clientID,
		JobID:      uuid.NewString(),
		ChunkSize:  1000,
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
