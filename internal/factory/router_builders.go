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

	routedSender := sender.NewRoutedSender(
		paymentTypeExchange,
		protowrappers.WrapAvgByTypeTransactions,
		protowrappers.ProtoSizer[*protobuf.AvgByTypeTransaction](),
		0,
		protoinserters.InsertAvgByTypeTransactionBatch,
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
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("OUTPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputWorkerAmount, err := getEnvIntStrict("OUTPUT_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	keys := make([]string, outputWorkerAmount)
	for i := 0; i < outputWorkerAmount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	microtransactionJoinExchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		microtransactionJoinExchange,
		protowrappers.WrapToMicrotransactionBatch,
		protowrappers.ProtoSizer[*protobuf.Microtransaction](),
		0,
		protoinserters.InsertMicrotransactionBatch,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.Microtransaction,
		sender.RoutedItem[*protobuf.Microtransaction],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       outputWorkerAmount,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_MICROTRANSACTION_BATCH,
		ExtractInputItems:  protoextractors.GetMicrotransactionBatchItems,
		Processor:          procesorrouters.NewMicrotransactionJoinRouter(keys),
		Sender:             routedSender,
	})
}

func buildMaxBankRouterToJoinWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("OUTPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputWorkerAmount, err := getEnvIntStrict("OUTPUT_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	keys := make([]string, outputWorkerAmount)
	for i := 0; i < outputWorkerAmount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	exchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		exchange,
		protowrappers.WrapMaxBankResults,
		protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
		0,
		protoinserters.InsertMaxBankResultBatch,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.MaxBankResult,
		sender.RoutedItem[*protobuf.MaxBankResult],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       outputWorkerAmount,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_MAX_BANK_RESULT_BATCH,
		ExtractInputItems:  protoextractors.GetMaxBankResultBatchItems,
		Processor:          procesorrouters.NewMaxBankToJoinRouter(keys),
		Sender:             routedSender,
	})
}

func buildAvgByTypeRouterToJoinWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("OUTPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputWorkerAmount, err := getEnvIntStrict("OUTPUT_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	keys := make([]string, outputWorkerAmount)
	for i := 0; i < outputWorkerAmount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	exchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		exchange,
		protowrappers.WrapAvgByTypeResults,
		protowrappers.ProtoSizer[*protobuf.AvgByTypeResult](),
		0,
		protoinserters.InsertAvgByTypeResultBatch,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.AvgByTypeResult,
		sender.RoutedItem[*protobuf.AvgByTypeResult],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       outputWorkerAmount,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_AVGBYTYPE_RESULT_BATCH,
		ExtractInputItems:  protoextractors.GetAvgByTypeResultBatchItems,
		Processor:          procesorrouters.NewAvgByTypeJoinRouter(keys),
		Sender:             routedSender,
	})
}

func buildSuspiciousPathRouterToJoinWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("OUTPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputWorkerAmount, err := getEnvIntStrict("OUTPUT_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	keys := make([]string, outputWorkerAmount)
	for i := 0; i < outputWorkerAmount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}

	exchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom)
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		exchange,
		protowrappers.WrapSuspiciousPaths,
		protowrappers.ProtoSizer[*protobuf.SuspiciousPath](),
		0,
		protoinserters.InsertSuspiciousPathBatch,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[
		*protobuf.SuspiciousPath,
		sender.RoutedItem[*protobuf.SuspiciousPath],
	]{
		Mom:                mom,
		id:                 id,
		workerCount:        workerCount,
		workerExchangeName: workerExchangeName,
		expectedEOFs:       outputWorkerAmount,
		InputQueueName:     inQ,
		InputMessageType:   protobuf.MessageType_SUSPICIOUS_PATH_BATCH,
		ExtractInputItems:  protoextractors.GetSuspiciousPathBatchItems,
		Processor:          procesorrouters.NewSuspiciousPathToJoinRouter(keys),
		Sender:             routedSender,
	})
}
