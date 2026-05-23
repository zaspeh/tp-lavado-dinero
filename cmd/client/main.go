package main

import (
	"log/slog"
	"os"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/logging"
	"github.com/zaspeh/tp-lavado-dinero/internal/factory"
)

func run() int {
	logging.InitDefaultLogger()
	client, err := factory.CreateClient()
	if err != nil {
		slog.Error("while loading config", "err", err)
		return 1
	}

	if err := client.Run(); err != nil {
		slog.Error("client stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
