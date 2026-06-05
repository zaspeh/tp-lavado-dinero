package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserter"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/groupers/maxbank"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

func buildStatefulWorkerInputExchangeOutputQueue() (workers.Worker, error) {
	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	inputExchange, outputQueue, err := createInputExchangeOutputQueue()
	if err != nil {
		return nil, err
	}

	newCoordinator := coordinator.NewAloneCoordinator()

	receiver := receiver.NewSingleReceiver(
		inputExchange,
		protobuf.MessageType_MAXBANK_BATCH,
		protoextractors.GetMaxBankBatchItems,
	)

	sender := sender.NewSingleSender(
		outputQueue,
		protowrappers.WrapMaxBankResults,
		protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
		maxBatchWeight,
		protoinserter.InsertMaxBankResultBatch,
	)

	processor := maxbank.NewMaxBankProcessor()
	engine := engine.NewStatefulEngine(receiver, sender, processor, newCoordinator)
	worker := worker.NewWorker()
	worker.AddEngine(engine)
	return worker, nil
}
