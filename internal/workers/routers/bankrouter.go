package routers

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type BackRouterConfig struct {
	MomHost           string
	MomPort           int
	InputQueueName    string
	BankExchangerName string
	MaxWorkersAmount  int
}

type BankRouter struct {
	inputQueue         middleware.Middleware
	bankExchangerQueue middleware.ExchangeMiddleware
	maxWorkersAmount   int
}
