package factory

import (
	"fmt"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	processorrouters "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/routers"
	procesorrouters "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/routers/joiners"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

func buildBankRouterWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	maxBankExchangeName, err := getEnvStrict("MAX_BANK_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	maxBankoutputWorkerAmount, err := getEnvIntStrict("MAX_BANK_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	maxBankExchangeKeys := make([]string, maxBankoutputWorkerAmount)
	for i := range maxBankExchangeKeys {
		maxBankExchangeKeys[i] = fmt.Sprintf("%s.%d", maxBankExchangeName, i)
	}

	maxBankExchange, err := m.CreateExchangeMiddleware(maxBankExchangeName, maxBankExchangeKeys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		maxBankExchange,
		protowrappers.WrapMaxBank,
		protowrappers.ProtoSizer[*protobuf.MaxBank](),
		0,
		protoinserters.InsertMaxBankBatch,
		workerExchangeName,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.MaxBank,
		sender.RoutedItem[*protobuf.MaxBank],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       2,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_MAXBANK_BATCH,
		ExtractInputItems:  protoextractors.GetMaxBankBatchItems,
		Processor:          processorrouters.NewMaxBankRouter(maxBankExchangeKeys),
		Sender:             routedSender,
	})
}

func buildOriginDestinationRouterWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	groupByOriginoutputWorkerAmount, err := getEnvIntStrict("GROUP_BY_ORIGIN_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	groupByExchangeName, err := getEnvStrict("GROUP_BY_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	groupByDestinationoutputWorkerAmount, err := getEnvIntStrict("GROUP_BY_DESTINATION_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	originDestinationRouterKeys := make([]string, groupByOriginoutputWorkerAmount+groupByDestinationoutputWorkerAmount)
	for i := 0; i < groupByOriginoutputWorkerAmount; i++ {
		originDestinationRouterKeys[i] = fmt.Sprintf("origin.%d", i)
	}
	for i := 0; i < groupByDestinationoutputWorkerAmount; i++ {
		originDestinationRouterKeys[groupByOriginoutputWorkerAmount+i] = fmt.Sprintf("destination.%d", i)
	}

	groupByExchange, err := m.CreateExchangeMiddleware(groupByExchangeName, originDestinationRouterKeys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		groupByExchange,
		protowrappers.WrapScatterGather,
		protowrappers.ProtoSizer[*protobuf.ScatterGather](),
		0,
		protoinserters.InsertScatterGatherBatch,
		workerExchangeName,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.ScatterGather,
		sender.RoutedItem[*protobuf.ScatterGather],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       0,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_SCATTERGATHER_BATCH,
		ExtractInputItems:  protoextractors.GetScatterGatherBatchItems,
		Processor:          processorrouters.NewOriginDestinationRouter(originDestinationRouterKeys[:groupByOriginoutputWorkerAmount], originDestinationRouterKeys[groupByOriginoutputWorkerAmount:]),
		Sender:             routedSender,
	})
}

func buildPaymentTypeRouterWorker() (workers.Worker, error) {
	momConfig, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}
	queues, err := createQueues([]string{"INPUT_QUEUE_NAME"}, momConfig)
	if err != nil {
		return nil, err
	}

	inputQueue := queues[0]

	paymentTypeExchange, paymentTypeKeys, err := createExchangeOutput(momConfig, "PAYMENT_TYPE_EXCHANGE_NAME", "AVG_BY_TYPE_WORKER_AMOUNT")
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	coordinator, err := getCoordinator()
	if err != nil {
		inputQueue.Close()
		paymentTypeExchange.Close()
		return nil, err
	}
	workerExchangeName, err := getEnvStrict("WORKER_EXCHANGE_NAME")
	if err != nil {
		inputQueue.Close()
		paymentTypeExchange.Close()
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		paymentTypeExchange,
		protowrappers.WrapAvgByTypeTransactions,
		protowrappers.ProtoSizer[*protobuf.AvgByTypeTransaction](),
		0,
		protoinserters.InsertAvgByTypeTransactionBatch,
		workerExchangeName,
	)

	receiver := r.NewSingleReceiver(
		inputQueue,
		protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH,
		protoextractors.GetAvgByTypeTransactionBatchItems,
	)

	// TODO: ESTA MAL, HACER PROCESADOR ESPECIFICO
	processor := processorrouters.NewRouterProcessor(
		paymentTypeKeys,
		func(item *protobuf.AvgByTypeTransaction) string {
			return item.GetPaymentFormat()
		},
	)

	engine := engine.NewStatelessEngine(receiver, routedSender, processor, coordinator)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		engine.Shutdown()
		return nil, err
	}

	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}

func buildIntermediaryRouterWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("AGGREGATE_BY_INTERMEDIARY_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryoutputWorkerAmount, err := getEnvIntStrict("AGGREGATE_BY_INTERMEDIARY_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()

	if err != nil {
		return nil, err
	}

	inputWorkersAmount, err := getEnvIntStrict("INPUT_WORKERS_AMOUNT")
	if err != nil {
		return nil, err
	}

	keys := make([]string, aggregateByIntermediaryoutputWorkerAmount)
	for i := 0; i < aggregateByIntermediaryoutputWorkerAmount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	AggregateByIntermediaryExchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		AggregateByIntermediaryExchange,
		protowrappers.WrapIntermediaryPair,
		protowrappers.ProtoSizer[*protobuf.IntermediaryPair](),
		0,
		protoinserters.InsertIntermediaryPairBatch,
		workerExchangeName,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.GroupedAccounts,
		sender.RoutedItem[*protobuf.IntermediaryPair],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       inputWorkersAmount,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_GROUPED_ACCOUNTS_BATCH,
		ExtractInputItems:  protoextractors.GetGroupedAccountsBatchItems,
		Processor:          processorrouters.NewIntermediaryRouter(keys),
		Sender:             routedSender,
	})

}

func buildMicrotransactionRouterToJoinWorker() (workers.Worker, error) {
	return buildRoutedToJoinWorker(routedWorkerConfig[
		*protobuf.Microtransaction,
		*protobuf.MicrotransactionBatch,
	]{
		InputMessageType: protobuf.MessageType_MICROTRANSACTION_BATCH,
		ExtractInput:     protoextractors.GetMicrotransactionBatchItems,
		MakeProcessor:    microtxRouterFactory,
		Wrapper:          protowrappers.WrapToMicrotransactionBatch,
		Sizer:            protowrappers.ProtoSizer[*protobuf.Microtransaction](),
		Inserter:         protoinserters.InsertMicrotransactionBatch,
	})
}

func microtxRouterFactory(keys []string) RoutedProcessor[*protobuf.Microtransaction] {
	return procesorrouters.NewMicrotransactionJoinRouter(keys)
}

func buildMaxBankRouterToJoinWorker() (workers.Worker, error) {
	return buildRoutedToJoinWorker(routedWorkerConfig[
		*protobuf.MaxBankResult,
		*protobuf.MaxBankResultBatch,
	]{
		InputMessageType: protobuf.MessageType_MAX_BANK_RESULT_BATCH,
		ExtractInput:     protoextractors.GetMaxBankResultBatchItems,
		MakeProcessor:    maxBankRouterFactory,
		Wrapper:          protowrappers.WrapMaxBankResults,
		Sizer:            protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
		Inserter:         protoinserters.InsertMaxBankResultBatch,
	})
}

func maxBankRouterFactory(keys []string) RoutedProcessor[*protobuf.MaxBankResult] {
	return procesorrouters.NewMaxBankToJoinRouter(keys)
}

func buildAvgByTypeRouterToJoinWorker() (workers.Worker, error) {
	return buildRoutedToJoinWorker(routedWorkerConfig[
		*protobuf.AvgByTypeResult,
		*protobuf.AvgByTypeResultBatch,
	]{
		InputMessageType: protobuf.MessageType_AVGBYTYPE_RESULT_BATCH,
		ExtractInput:     protoextractors.GetAvgByTypeResultBatchItems,
		MakeProcessor:    avgByTypeRouterFactory,
		Wrapper:          protowrappers.WrapAvgByTypeResults,
		Sizer:            protowrappers.ProtoSizer[*protobuf.AvgByTypeResult](),
		Inserter:         protoinserters.InsertAvgByTypeResultBatch,
	})
}

func avgByTypeRouterFactory(keys []string) RoutedProcessor[*protobuf.AvgByTypeResult] {
	return procesorrouters.NewAvgByTypeJoinRouter(keys)
}

func buildSuspiciousPathRouterToJoinWorker() (workers.Worker, error) {
	return buildRoutedToJoinWorker(routedWorkerConfig[
		*protobuf.SuspiciousPath,
		*protobuf.SuspiciousPathBatch,
	]{
		InputMessageType: protobuf.MessageType_SUSPICIOUS_PATH_BATCH,
		ExtractInput:     protoextractors.GetSuspiciousPathBatchItems,
		MakeProcessor:    suspiciousPathRouterFactory,
		Wrapper:          protowrappers.WrapSuspiciousPaths,
		Sizer:            protowrappers.ProtoSizer[*protobuf.SuspiciousPath](),
		Inserter:         protoinserters.InsertSuspiciousPathBatch,
	})
}

func suspiciousPathRouterFactory(keys []string) RoutedProcessor[*protobuf.SuspiciousPath] {
	return procesorrouters.NewSuspiciousPathToJoinRouter(keys)
}

func buildConvertedAmountRouterToJoinWorker() (workers.Worker, error) {
	return buildRoutedToJoinWorker(routedWorkerConfig[
		*protobuf.ConvertedAmount,
		*protobuf.ConvertedAmountBatch,
	]{
		InputMessageType: protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		ExtractInput:     protoextractors.GetConvertedAmountBatchItems,
		MakeProcessor:    convertedAmountRouterFactory,
		Wrapper:          protowrappers.WrapConvertedAmounts,
		Sizer:            protowrappers.ProtoSizer[*protobuf.ConvertedAmount](),
		Inserter:         protoinserters.InsertConvertedAmountBatch,
	})
}

func convertedAmountRouterFactory(keys []string) RoutedProcessor[*protobuf.ConvertedAmount] {
	return procesorrouters.NewConvertedAmountJoinRouter(keys)
}
