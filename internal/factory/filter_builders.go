package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters"
)

func buildCurrencyFilterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	microQ, err := getEnvStrict("MICROTRANSACTION_FILTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	routerQ, err := getEnvStrict("BANK_ROUTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	periodQ, err := getEnvStrict("PERIOD_FILTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	currency, err := getEnvStrict("CURRENCY_TO_FILTER")
	if err != nil {
		return nil, err
	}

	config := filters.CurrencyFilterConfig{
		InputQueueName:                  inQ,
		MicrotransactionFilterQueueName: microQ,
		BankRouterQueueName:             routerQ,
		PeriodFilterQueueName:           periodQ,
		MomHost:                         host,
		MomPort:                         port,
		CurrencyToFilter:                currency,
	}

	return filters.NewCurrencyFilter(config)
}

func buildAmountFilterWorker() (workers.Worker, error) {
	return nil, nil
}
