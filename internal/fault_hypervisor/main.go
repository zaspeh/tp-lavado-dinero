package main

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/factory"
)

func main() {
	slog.Info("fault hypervisor started")
	hypervisor, err := factory.CreateFaultHypervisor()
	if err != nil {
		slog.Error("failed to build fault hypervisor", "error", err)
		return
	}

	if err := hypervisor.Run(); err != nil {
		slog.Error("fault hypervisor encountered an error", "error", err)
	}
}
