package main

import (
	"log/slog"
	"os"

	"github.com/zaspeh/tp-lavado-dinero/internal/factory"
)

func run() int {
	gateway, err := factory.CreateGateway()
	if err != nil {
		slog.Error("while loading config", "err", err)
		return 1
	}

	if err := gateway.Run(); err != nil {
		slog.Error("gateway stopped with error", "err", err)
		return 1
	}

	return 0
}

func main() {
	os.Exit(run())
}
