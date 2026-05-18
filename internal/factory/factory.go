package factory

import "github.com/zaspeh/tp-lavado-dinero/internal/workers"

func CreateWorker(workerType string) (workers.Worker, error) {
	switch workerType {
	case "CURRENCY_FILTER":
		return buildCurrencyFilterWorker()
	default:
		return nil, nil
	}
}
