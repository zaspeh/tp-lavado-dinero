package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/client"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
)

func CreateWorker(workerType string) (workers.Worker, error) {
	switch workerType {
	case "CURRENCY_FILTER":
		return buildCurrencyFilterWorker()
	case "BANK_ROUTER":
		return buildBankRouterWorker()
	case "MAX_BANK":
		return buildMaxBankWorker()
	case "MAX_BANK_JOIN":
		return buildMaxBankJoinWorker()
	default:
		return nil, nil
	}
}

func CreateClient() (*client.Client, error) {
	return buildClient()
}

func CreateGateway() (*gateway.Gateway, error) {
	return buildGateway()
}
