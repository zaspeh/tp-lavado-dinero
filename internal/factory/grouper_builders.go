package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/groupers/origindestination"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/groupers/maxbank"
)

func buildMaxBankWorker() (workers.Worker, error) {
	return buildStatefulWorkerInputExchangeOutputQueue(
		InputExchangeOutputQueueConfig[*protobuf.MaxBank, *protobuf.MaxBankResult, *protobuf.MaxBankResultBatch]{
			ReceivedMessageType: protobuf.MessageType_MAXBANK_BATCH,
			Extractor:           protoextractors.GetMaxBankBatchItems,
			Wrapper:             protowrappers.WrapMaxBankResults,
			Sizer:               protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
			Inserter:            protoinserters.InsertMaxBankResultBatch,
			processor:           maxbank.NewMaxBankProcessor(),
		},
	)
}

func buildGroupByOriginWorker() (workers.Worker, error) {

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
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

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := origindestination.GroupByOriginWorkerConfig{
		ID:                id,
		MomHost:           host,
		MomPort:           port,
		InputExchangeName: inputExchangeName,
		OutputQueueName:   outputQueueName,
		MaxBatchWeight:    maxBatchWeight,
	}

	return origindestination.NewGroupByOriginWorker(config)
}

func buildGroupByDestinationWorker() (workers.Worker, error) {

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
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

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := origindestination.GroupByDestinationWorkerConfig{
		ID:                id,
		MomHost:           host,
		MomPort:           port,
		InputExchangeName: inputExchangeName,
		OutputQueueName:   outputQueueName,
		MaxBatchWeight:    maxBatchWeight,
	}

	return origindestination.NewGroupByDestinationWorker(config)
}
