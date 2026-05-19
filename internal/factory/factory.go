package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/client"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
)

func CreateWorker(workerType string) (workers.Worker, error) {
	switch workerType {
	case "CURRENCY_FILTER":
		return buildCurrencyFilterWorker()
	case "BANK_ROUTER":
		return buildBankRouterWorker()
	default:
		return nil, nil
	}
}

func CreateClient() (*client.Client, error) {
	return buildClient()
}
