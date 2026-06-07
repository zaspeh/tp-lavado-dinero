package main

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/factory"
)

func main() {
	slog.Info("fault hypervisor started")

	hypervisor, err := factory.CreateWorker("FAULT_HYPERVISOR")
	if err != nil {
		slog.Error("failed to build fault hypervisor", "error", err)
		return
	}

	err = hypervisor.Run()
	if err != nil {
		slog.Error("failed to start fault hypervisor", "error", err)
		return
	}

	select {}
}
