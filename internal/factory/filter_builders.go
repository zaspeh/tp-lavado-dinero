package factory

import (
	"fmt"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
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

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	coordinator, err := getCoordinator(maxBatchWeight, 1)
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

	_, _, namespace, err := getCoordinationInformationFromEnv()
	if err != nil {
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
		maxBatchWeight,
		protoinserters.InsertMicrotransactionBatch,
		namespace,
	)

	senderMaxBankRouter := s.NewSingleSender(
		queues[2],
		protowrappers.WrapTransactionToMaxBankBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		maxBatchWeight,
		protoinserters.InsertMaxBankBatch,
		namespace,
	)

	senderPaymentTypePeriod := s.NewSingleSender(
		queues[3],
		protowrappers.WrapTransactionToPeriodFilterBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		maxBatchWeight,
		protoinserters.InsertPeriodFilterBatch,
		namespace,
	)

	senderScatterGatherPeriod := s.NewSingleSender(
		queues[4],
		protowrappers.WrapTransactionToPeriodFilterBatch,
		protowrappers.ProtoSizer[*protobuf.Transaction](),
		maxBatchWeight,
		protoinserters.InsertPeriodFilterBatch,
		namespace,
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

	queueAliases := []string{
		"RAW_INPUT_QUEUE_NAME",
		"PAYMENT_TYPE_PERIOD_FILTER_QUEUE_NAME",
		"SCATTER_GATHER_PERIOD_FILTER_QUEUE_NAME",
		"PAYMENT_TYPE_FILTER_QUEUE_NAME",
		"PAYMENT_TYPE_ROUTER_QUEUE_NAME",
		"ORIGIN_DESTINATION_ROUTER_QUEUE_NAME",
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	queues, err := createQueues(queueAliases, connSettings)
	if err != nil {
		return nil, err
	}

	rawInputQueue := queues[0]
	paymentTypePeriodInputQueue := queues[1]
	scatterGatherPeriodInputQueue := queues[2]
	paymentTypeFilterQueue := queues[3]
	paymentTypeRouterQueue := queues[4]
	originDestinationRouterQueue := queues[5]

	scatterGatherPeriod, err := buildPeriodFromEnv(
		"SCATTER_GATHER_PERIOD_START",
		"SCATTER_GATHER_PERIOD_END",
	)
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	paymentTypePeriod, err := buildPeriodFromEnv(
		"PAYMENT_TYPE_PERIOD_START",
		"PAYMENT_TYPE_PERIOD_END",
	)
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	query3Period1, err := buildPeriodFromEnv(
		"QUERY3_PERIOD_1_START",
		"QUERY3_PERIOD_1_END",
	)
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	query3Period2, err := buildPeriodFromEnv(
		"QUERY3_PERIOD_2_START",
		"QUERY3_PERIOD_2_END",
	)
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	rawCoordinator, err := buildPeriodFilterCoordinator(connSettings, id, workerCount, fmt.Sprintf("%s.q5_raw", workerExchangeName), maxBatchWeight)
	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	query3Coordinator, err := buildPeriodFilterCoordinator(connSettings, id, workerCount, fmt.Sprintf("%s.q3_periods", workerExchangeName), maxBatchWeight)
	if err != nil {
		rawCoordinator.Close()
		closeQueues(queues)
		return nil, err
	}

	query4Coordinator, err := buildPeriodFilterCoordinator(connSettings, id, workerCount, fmt.Sprintf("%s.q4_scatter", workerExchangeName), maxBatchWeight)
	if err != nil {
		rawCoordinator.Close()
		query3Coordinator.Close()
		closeQueues(queues)
		return nil, err
	}

	rawProcessor := filterprocessor.NewToConvertPeriodFilterProcessor(paymentTypePeriod)
	query3Processor := filterprocessor.NewAvgByTypePeriodFilterProcessor(query3Period1, query3Period2)
	query4Processor := filterprocessor.NewScatterGatherPeriodFilterProcessor(scatterGatherPeriod)

	rawEngine, err := buildSingleReceiverSingleSenderEngine(
		singleReceiverSingleSenderEngineConfig[*protobuf.ToConvertTransaction, *protobuf.ToConvertPeriodFiltered, *protobuf.ToConvertPeriodFilteredBatch]{
			InputQueue:          rawInputQueue,
			OutputQueue:         paymentTypeFilterQueue,
			ReceivedMessageType: protobuf.MessageType_TO_CONVERT_TRANSACTION_BATCH,
			Extractor:           protoextractors.GetToConvertTransactionBatchItems,
			Wrapper:             protowrappers.WrapToConvertPeriodFiltered,
			Sizer:               protowrappers.ProtoSizer[*protobuf.ToConvertPeriodFiltered](),
			Inserter:            protoinserters.InsertToConvertPeriodFilteredBatch,
			Processor:           rawProcessor,
			Coordinator:         rawCoordinator,
			MaxBatchWeight:      maxBatchWeight,
		},
	)

	if err != nil {
		closeQueues(queues)
		return nil, err
	}

	query3Engine, err := buildSingleReceiverSingleSenderEngine(
		singleReceiverSingleSenderEngineConfig[*protobuf.PeriodFilter, *protobuf.AvgByTypeTransaction, *protobuf.AvgByTypeTransactionBatch]{
			InputQueue:          paymentTypePeriodInputQueue,
			OutputQueue:         paymentTypeRouterQueue,
			ReceivedMessageType: protobuf.MessageType_PERIOD_FILTER_BATCH,
			Extractor:           protoextractors.GetPeriodFilterBatchItems,
			Wrapper:             protowrappers.WrapAvgByTypeTransactions,
			Sizer:               protowrappers.ProtoSizer[*protobuf.AvgByTypeTransaction](),
			Inserter:            protoinserters.InsertAvgByTypeTransactionBatch,
			Processor:           query3Processor,
			Coordinator:         query3Coordinator,
			MaxBatchWeight:      maxBatchWeight,
		},
	)

	if err != nil {
		rawEngine.Shutdown()
		closeQueues(queues)
		return nil, err
	}

	query4Engine, err := buildSingleReceiverSingleSenderEngine(
		singleReceiverSingleSenderEngineConfig[*protobuf.PeriodFilter, *protobuf.ScatterGather, *protobuf.ScatterGatherBatch]{
			InputQueue:          scatterGatherPeriodInputQueue,
			OutputQueue:         originDestinationRouterQueue,
			ReceivedMessageType: protobuf.MessageType_PERIOD_FILTER_BATCH,
			Extractor:           protoextractors.GetPeriodFilterBatchItems,
			Wrapper:             protowrappers.WrapScatterGather,
			Sizer:               protowrappers.ProtoSizer[*protobuf.ScatterGather](),
			Inserter:            protoinserters.InsertScatterGatherBatch,
			Processor:           query4Processor,
			Coordinator:         query4Coordinator,
			MaxBatchWeight:      maxBatchWeight,
		},
	)

	if err != nil {
		rawEngine.Shutdown()
		query3Engine.Shutdown()
		closeQueues(queues)
		return nil, err
	}

	heartbeat, err := buildHeartbeatPublisher()
	if err != nil {
		rawEngine.Shutdown()
		query3Engine.Shutdown()
		query4Engine.Shutdown()
		return nil, err
	}

	workerInstance := worker.NewWorker(heartbeat)
	workerInstance.AddEngine(rawEngine)
	workerInstance.AddEngine(query3Engine)
	workerInstance.AddEngine(query4Engine)

	return workerInstance, nil
}

func buildPeriodFilterCoordinator(
	connSettings m.ConnSettings,
	id int,
	workerCount int,
	workerExchangeName string,
	maxBatchWeight int,
) (*c.EOFCoordinator, error) {
	return c.NewEOFCoordinator(c.EOFCoordinatorConfig{
		PeersExchangeName: workerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          id,
		WorkerCount:       workerCount,
		MaxBatchWeight:    maxBatchWeight,
	})
}

func buildFormatFilterWorker() (workers.Worker, error) {
	allowedFormats, err := getEnvStringSliceStrict("VALID_PAYMENT_FORMATS")
	if err != nil {
		return nil, err
	}
	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
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
			MaxBatchWeight:      maxBatchWeight,
		},
	)
}

func buildAmountConvertedFilterWorker() (workers.Worker, error) {
	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}
	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
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
			MaxBatchWeight:      maxBatchWeight,
		},
	)
}

func buildAmountFilterWorker() (workers.Worker, error) {
	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}
	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
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
			MaxBatchWeight:      maxBatchWeight,
		},
	)
}
