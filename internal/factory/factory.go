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
	case "ORIGIN_DESTINATION_ROUTER":
		return buildOriginDestinationRouterWorker()
	case "PERIOD_FILTER":
		return buildPeriodFilterWorker()
	case "GROUP_BY_ORIGIN":
		return buildGroupByOriginWorker()
	case "GROUP_BY_DESTINATION":
		return buildGroupByDestinationWorker()
	case "INTERMEDIARY_ROUTER":
		return buildIntermediaryRouterWorker()
	case "MICROTRANSACTION_JOIN":
		return buildMicrotransactionJoinWorker()
	case "AMOUNT_FILTER":
		return buildAmountFilterWorker()
	case "PAYMENT_TYPE_ROUTER":
		return buildPaymentTypeRouterWorker()
	case "AVG_BY_TYPE":
		return buildAvgByTypeWorker()
	case "AVG_BY_TYPE_JOIN":
		return buildAvgByTypeJoinWorker()
	case "FORMAT_FILTER":
		return buildFormatFilterWorker()
	case "CURRENCY_CONVERTER":
		return buildCurrencyConverterWorker()
	case "AMOUNT_CONVERTED_FILTER":
		return buildAmountConvertedFilterWorker()
	case "CONVERSION_JOIN":
		return buildConvertedMicroPaymentJoinWorker()
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
