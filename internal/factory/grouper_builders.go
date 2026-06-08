package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/groupers/maxbank"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/groupers/origindestination"
)

func buildMaxBankWorker() (workers.Worker, error) {
	return buildStatefulWorkerInputExchangeOutputQueue(
		InputExchangeOutputQueueStatefulConfig[*protobuf.MaxBank, *protobuf.MaxBankResult, *protobuf.MaxBankResultBatch]{
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
	return buildStatefulWorkerInputExchangeOutputQueue(
		InputExchangeOutputQueueStatefulConfig[*protobuf.ScatterGather, *protobuf.GroupedAccounts, *protobuf.GroupedAccountsBatch]{
			ReceivedMessageType: protobuf.MessageType_SCATTERGATHER_BATCH,
			Extractor:           protoextractors.GetScatterGatherBatchItems,
			Wrapper:             protowrappers.WrapGroupedAccounts,
			Sizer:               protowrappers.ProtoSizer[*protobuf.GroupedAccounts](),
			Inserter:            protoinserters.InsertGroupedAccountsBatch,
			processor:           origindestination.NewGroupByOriginProcessor(),
			keys:                "origin",
		},
	)
}

func buildGroupByDestinationWorker() (workers.Worker, error) {
	return buildStatefulWorkerInputExchangeOutputQueue(
		InputExchangeOutputQueueStatefulConfig[*protobuf.ScatterGather, *protobuf.GroupedAccounts, *protobuf.GroupedAccountsBatch]{
			ReceivedMessageType: protobuf.MessageType_SCATTERGATHER_BATCH,
			Extractor:           protoextractors.GetScatterGatherBatchItems,
			Wrapper:             protowrappers.WrapGroupedAccounts,
			Sizer:               protowrappers.ProtoSizer[*protobuf.GroupedAccounts](),
			Inserter:            protoinserters.InsertGroupedAccountsBatch,
			processor:           origindestination.NewGroupByDestinationProcessor(),
			keys:                "destination",
		},
	)
}
