package factory

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/conversionamountfilter"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/formatfilter"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/periodfilter"
)

func buildCurrencyFilterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

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

	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
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
		WorkerCount:                     workerCount,
		WorkerExchangeName:              workerExchangeName,
		ID:                              id,
	}

	return filters.NewCurrencyFilter(config)
}

func buildPeriodFilterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	usdInputQ, err := getEnvStrict("USD_INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	rawInputQ, err := getEnvStrict("RAW_INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	originDestinationRouterQ, err := getEnvStrict("ORIGIN_DESTINATION_ROUTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	paymentTypeQ, err := getEnvStrict("PAYMENT_TYPE_FILTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	avgByTypeQ1, err := getEnvStrict("AVG_BY_TYPE_PERIOD_1_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	avgByTypeQ2, err := getEnvStrict("AVG_BY_TYPE_PERIOD_2_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	paymentTypeRouterQ, err := getEnvStrict("PAYMENT_TYPE_ROUTER_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	avgByTypePeriod1, err := buildPeriodFromEnv(
		"AVG_BY_TYPE_PERIOD_1_START",
		"AVG_BY_TYPE_PERIOD_1_END",
	)
	if err != nil {
		return nil, err
	}

	avgByTypePeriod2, err := buildPeriodFromEnv(
		"AVG_BY_TYPE_PERIOD_2_START",
		"AVG_BY_TYPE_PERIOD_2_END",
	)
	if err != nil {
		return nil, err
	}

	scatterGatherPeriod, err := buildPeriodFromEnv(
		"SCATTER_GATHER_PERIOD_START",
		"SCATTER_GATHER_PERIOD_END",
	)
	if err != nil {
		return nil, err
	}

	paymentTypePeriod, err := buildPeriodFromEnv(
		"PAYMENT_TYPE_PERIOD_START",
		"PAYMENT_TYPE_PERIOD_END",
	)
	if err != nil {
		return nil, err
	}

	query3Period1, err := buildPeriodFromEnv(
		"QUERY3_PERIOD_1_START",
		"QUERY3_PERIOD_1_END",
	)
	if err != nil {
		return nil, err
	}

	query3Period2, err := buildPeriodFromEnv(
		"QUERY3_PERIOD_2_START",
		"QUERY3_PERIOD_2_END",
	)
	if err != nil {
		return nil, err
	}

	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := periodfilter.PeriodFilterWorkerConfig{
		UsdInputQueueName: usdInputQ,
		RawInputQueueName: rawInputQ,

		AvgByTypeQueueNames: []string{
			avgByTypeQ1,
			avgByTypeQ2,
		},

		AvgByTypePeriods: []periodfilter.Period{
			avgByTypePeriod1,
			avgByTypePeriod2,
		},

		ScatterGatherPeriod: scatterGatherPeriod,

		Query3Period1: query3Period1,
		Query3Period2: query3Period2,

		OriginDestinationRouterQueueName: originDestinationRouterQ,
		PaymentTypeRouterQueueName:       paymentTypeRouterQ,

		PaymentTypePeriod:          paymentTypePeriod,
		PaymentTypeFilterQueueName: paymentTypeQ,

		MomHost: host,
		MomPort: port,

		RawWorkerID:           id,
		RawWorkerCount:        workerCount,
		RawWorkerExchangeName: fmt.Sprintf("%s.q5_raw", workerExchangeName),

		Query4WorkerID:           id,
		Query4WorkerCount:        workerCount,
		Query4WorkerExchangeName: fmt.Sprintf("%s.q4_scatter", workerExchangeName),

		Query3WorkerID:           id,
		Query3WorkerCount:        workerCount,
		Query3WorkerExchangeName: fmt.Sprintf("%s.q3_periods", workerExchangeName),
	}

	return periodfilter.NewPeriodFilterWorker(config)
}

func buildAmountFilterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}

	config := filters.AmountFilterConfig{
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		MomHost:         host,
		MomPort:         port,
		AmountToFilter:  amountToFilter,
	}

	return filters.NewAmountFilter(config)
}

func buildFormatFilterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	allowedFormats, err := getEnvStringSliceStrict("VALID_PAYMENT_FORMATS")
	if err != nil {
		return nil, err
	}

	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := formatfilter.FormatFilterConfig{
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		MomHost:         host,
		MomPort:         port,
		AllowedFormats:  allowedFormats,
		WorkerID:        id,
		WorkerCount:     workerCount,
		WorkerExchange:  workerExchangeName,
	}

	return formatfilter.NewFormatFilterWorker(config)
}

func buildAvgByTypeWorker() (workers.Worker, error) {
	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := filters.AvgByTypeFilterConfig{
		ID: id,

		InputExchangeName: inputExchangeName,
		OutputQueueName:   outputQueueName,

		MomHost: host,
		MomPort: port,
	}

	return filters.NewAvgByTypeFilter(config)
}

func buildAmountConvertedFilterWorker() (workers.Worker, error) {
	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}

	workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := conversionamountfilter.AmountFilterConfig{
		InputQueueName:  inputQueueName,
		OutputQueueName: outputQueueName,
		MomHost:         host,
		MomPort:         port,
		AmountToFilter:  amountToFilter,
		WorkerID:        id,
		WorkerCount:     workerCount,
		WorkerExchange:  workerExchangeName,
	}

	return conversionamountfilter.NewAmountFilter(config)
}
