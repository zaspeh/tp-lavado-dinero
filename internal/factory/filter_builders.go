package factory

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/periodfilter"
	filterprocessor "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/filters"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

func buildCurrencyFilterWorker() (workers.Worker, error) {
	connSettings, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}
	queuesAlias := []string{
		"INPUT_QUEUE_NAME",
		"MICROTRANSACTION_FILTER_QUEUE_NAME",
		"BANK_ROUTER_QUEUE_NAME",
		"PAYMENT_TYPE_PERIOD_FILTER_QUEUE_NAME",
		"SCATTER_GATHER_PERIOD_FILTER_QUEUE_NAME",
	}

	queues, err := createQueues(queuesAlias, connSettings)
	if err != nil {
		return nil, err
	}

	currency, err := getEnvStrict("CURRENCY_TO_FILTER")
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	coordinator, err := getCoordinator()
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	heartbeat, err := buildHeartbeatPublisher()
	if err != nil {
		closeQueues(queues)
		coordinator.Close()
		return nil, err
	}

	receiver := r.NewSingleReceiver(
		queues[0],
		protobuf.MessageType_TRANSACTION_BATCH,
		protoextractors.GetTransactionBatchItems,
	)

	// TODO: PESO DEL BATCH BATCH
	senderMicroTransaction := s.NewSingleSender(
		queues[1],
		protowrappers.WrapTransactionToMicroTransactionBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		0,
		protoinserters.InsertMicrotransactionBatch,
	)

	senderMaxBankRouter := s.NewSingleSender(
		queues[2],
		protowrappers.WrapTransactionToMaxBankBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		0,
		protoinserters.InsertMaxBankBatch,
	)

	senderPaymentTypePeriod := s.NewSingleSender(
		queues[3],
		protowrappers.WrapTransactionToPeriodFilterBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		0,
		protoinserters.InsertPeriodFilterBatch,
	)

	senderScatterGatherPeriod := s.NewSingleSender(
		queues[4],
		protowrappers.WrapTransactionToPeriodFilterBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		0,
		protoinserters.InsertPeriodFilterBatch,
	)

	multisender := s.NewMultiSender(senderMicroTransaction, senderMaxBankRouter, senderPaymentTypePeriod, senderScatterGatherPeriod)

	processor := filterprocessor.NewCurrencyFilterProcessor(currency)

	engineInstance := engine.NewStatelessEngine(
		receiver,
		multisender,
		processor,
		coordinator,
	)

	workerInstance := worker.NewWorker(heartbeat)
	workerInstance.AddEngine(engineInstance)

	return workerInstance, nil

}

func buildPeriodFilterWorker() (workers.Worker, error) {
	connSettings, err := getMomConfigFromEnv()
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

	paymentTypeRouterQ, err := getEnvStrict("PAYMENT_TYPE_ROUTER_QUEUE_NAME")
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

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := periodfilter.PeriodFilterWorkerConfig{
		UsdInputQueueName: usdInputQ,
		RawInputQueueName: rawInputQ,

		ScatterGatherPeriod: scatterGatherPeriod,

		Query3Period1: query3Period1,
		Query3Period2: query3Period2,

		OriginDestinationRouterQueueName: originDestinationRouterQ,
		PaymentTypeRouterQueueName:       paymentTypeRouterQ,

		PaymentTypePeriod:          paymentTypePeriod,
		PaymentTypeFilterQueueName: paymentTypeQ,

		MomHost: connSettings.Hostname,
		MomPort: connSettings.Port,

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

func buildFormatFilterWorker() (workers.Worker, error) {
	allowedFormats, err := getEnvStringSliceStrict("VALID_PAYMENT_FORMATS")
	if err != nil {
		return nil, err
	}
	return buildStatelessWorkerInputQueueOutputQueue(
		InputQueueOutputQueueStatelessConfig[*protobuf.ToConvertPeriodFiltered, *protobuf.ToConvertTypeFilteredPayment, *protobuf.ToConvertTypeFilteredPaymentBatch]{
			ReceivedMessageType: protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED_BATCH,
			Wrapper:             protowrappers.WrapToConvertTypeFilteredPayment,
			Extractor:           protoextractors.GetToConvertPeriodFilteredItems,
			Inserter:            protoinserters.InsertToConvertTypeFilteredPaymentBatch,
			Sizer:               protowrappers.ProtoSizer[*protobuf.ToConvertTypeFilteredPayment](),
			Processor:           filterprocessor.NewFormatFilterProcessor(allowedFormats),
		},
	)
}

func buildAvgByTypeWorker() (workers.Worker, error) {
	inputExchangeName, outputQueueName, err := createInputExchangeOutputQueue("")
	if err != nil {
		return nil, err
	}
	config := filters.AvgByTypeFilterConfig{
		InputExchange: inputExchangeName,
		OutputQueue:   outputQueueName,
	}

	return filters.NewAvgByTypeFilter(config)
}

func buildAmountConvertedFilterWorker() (workers.Worker, error) {
	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}

	return buildStatelessWorkerInputQueueOutputQueue(
		InputQueueOutputQueueStatelessConfig[*protobuf.ConvertedAmount, *protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]{
			ReceivedMessageType: protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
			Wrapper:             protowrappers.WrapConvertedAmounts,
			Extractor:           protoextractors.GetConvertedAmountBatchItems,
			Inserter:            protoinserters.InsertConvertedAmountBatch,
			Sizer:               protowrappers.ProtoSizer[*protobuf.ConvertedAmount](),
			Processor:           filterprocessor.NewAmountFilterProcessor[*protobuf.ConvertedAmount](amountToFilter),
		},
	)
}

func buildAmountFilterWorker() (workers.Worker, error) {
	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}

	return buildStatelessWorkerInputQueueOutputQueue(
		InputQueueOutputQueueStatelessConfig[*protobuf.Microtransaction, *protobuf.Microtransaction, *protobuf.MicrotransactionBatch]{
			ReceivedMessageType: protobuf.MessageType_MICROTRANSACTION_BATCH,
			Wrapper:             protowrappers.WrapToMicrotransactionBatch,
			Extractor:           protoextractors.GetMicrotransactionBatchItems,
			Inserter:            protoinserters.InsertMicrotransactionBatch,
			Sizer:               protowrappers.ProtoSizer[*protobuf.Microtransaction](),
			Processor:           filterprocessor.NewAmountFilterProcessor[*protobuf.Microtransaction](amountToFilter),
		},
	)
}
