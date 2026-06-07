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

	if hypervisor == nil {
		panic("hypervisor is nil")
	}

	if hypervisor.HeartbeatQueue == nil {
		panic("heartbeat queue is nil")
	}

	err = hypervisor.Run()
	if err != nil {
		slog.Error("fault hypervisor encountered an error", "error", err)
	}

	select {}
}
