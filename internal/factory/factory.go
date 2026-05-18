package factory

import "github.com/zaspeh/tp-lavado-dinero/internal/workers"

func CreateWorker(workerType string) workers.Worker {
	switch workerType {
	case "CURRENCY_FILTER":
		return nil
	default:
		return nil
	}
}
