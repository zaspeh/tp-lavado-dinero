package main

import (
	"log/slog"
	"os"

	"github.com/zaspeh/tp-lavado-dinero/internal/factory"
)

func run() int {
	workerType := os.Getenv("WORKER_TYPE")
	worker, err := factory.CreateWorker(workerType)
	if err != nil {
		slog.Error("while loading worker", "err", err)
		return 1
	}

	if err = worker.Run(); err != nil {
		slog.Error("worker stopped with error", "err", err)
		return 1
	}
	return 0
}

func main() {
	os.Exit(run())
}
