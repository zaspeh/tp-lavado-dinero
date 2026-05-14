package routers

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type BackRouterConfig struct {
	MomHost             string
	MomPort             int
	InputQueueName      string
	MaxBankExchangeName string
	MaxBankWorkerAmount int
}

type BankRouter struct {
	inputQueue          middleware.Middleware
	maxBankExchange     *middleware.ExchangeMiddleware
	maxBankExchangeKeys []string
	maxWorkersAmount    int
}

func NewBankRouter(config BackRouterConfig) (*BankRouter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	maxBankExchangeKeys := make([]string, config.MaxBankWorkerAmount)
	for i := range maxBankExchangeKeys {
		maxBankExchangeKeys[i] = fmt.Sprintf("%s.%d", config.MaxBankExchangeName, i)
	}

	maxBankExchange, err := middleware.CreateExchangeMiddleware(config.MaxBankExchangeName, maxBankExchangeKeys, connSettings)
	if err != nil {
		// TODO: verificar error de cierre?
		inputQueue.Close()
		return nil, err
	}

	return &BankRouter{
		inputQueue:          inputQueue,
		maxBankExchange:     maxBankExchange,
		maxBankExchangeKeys: maxBankExchangeKeys,
		maxWorkersAmount:    config.MaxBankWorkerAmount,
	}, nil
}
