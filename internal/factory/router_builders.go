package factory

import (
	"fmt"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserter"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	processorrouters "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/routers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/routers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
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

	maxBankWorkerAmount, err := getEnvIntStrict("MAX_BANK_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	maxBankExchangeKeys := make([]string, maxBankWorkerAmount)
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
		protoinserter.InsertMaxBankBatch,
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

	groupByOriginExchangeName, err := getEnvStrict("GROUP_BY_ORIGIN_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	groupByOriginWorkerAmount, err := getEnvIntStrict("GROUP_BY_ORIGIN_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	groupByDestinationExchangeName, err := getEnvStrict("GROUP_BY_DESTINATION_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	groupByDestinationWorkerAmount, err := getEnvIntStrict("GROUP_BY_DESTINATION_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}
	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()

	if err != nil {
		return nil, err
	}

	config := routers.OriginDestinationRouterConfig{
		ID:                              id,
		InputQueueName:                  inQ,
		GroupByOriginExchangeName:       groupByOriginExchangeName,
		GroupByDestinationExchangeName:  groupByDestinationExchangeName,
		GroupByOriginWorkersAmount:      groupByOriginWorkerAmount,
		GroupByDestinationWorkersAmount: groupByDestinationWorkerAmount,
		MomHost:                         host,
		MomPort:                         port,
		WorkerCount:                     workerCount,
		WorkerExchangeName:              workerExchangeName,
	}

	return routers.NewOriginDestinationRouter(config)
}

func buildPaymentTypeRouterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueue, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("PAYMENT_TYPE_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	avgByTypeWorkerAmount, err := getEnvIntStrict("AVG_BY_TYPE_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	config := routers.PaymentTypeRouterConfig{
		InputQueueName:          inputQueue,
		PaymentTypeExchangeName: exchangeName,
		AvgByTypeWorkerAmount:   avgByTypeWorkerAmount,
		MomHost:                 host,
		MomPort:                 port,
		WorkerID:                id,
		WorkerCount:             workerCount,
		WorkerExchangeName:      workerExchangeName,
	}

	return routers.NewPaymentTypeRouter(config)
}

func buildIntermediaryRouterWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputQueue, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("AGGREGATE_BY_INTERMEDIARY_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryWorkerAmount, err := getEnvIntStrict("AGGREGATE_BY_INTERMEDIARY_WORKER_AMOUNT")
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

	config := routers.IntermediaryRouterConfig{
		ID:                            id,
		InputQueueName:                inputQueue,
		AggregateByIntermediaryName:   exchangeName,
		AggregateByIntermediaryAmount: aggregateByIntermediaryWorkerAmount,
		MomHost:                       host,
		MomPort:                       port,
		WorkerCount:                   workerCount,
		WorkerExchangeName:            workerExchangeName,
		InputWorkersAmount:            inputWorkersAmount,
	}

	return routers.NewIntermediaryRouter(config)

}
