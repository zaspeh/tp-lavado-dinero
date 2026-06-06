package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

// Aclaracion de Tipos:
// T es el mensaje individual, por ejemplo, *proto.MaxBank
// V es el resultado individual del procesamiento, por ejemplo, *proto.MaxBankResult
// R es el batch resultante del procesamiento, por ejemplo, *proto.MaxBankResultBatch
type InputExchangeOutputQueueConfig[T, V, R any] struct {
	ReceivedMessageType protobuf.MessageType
	Extractor           func(*protobuf.MoneyLaundry) []T
	Wrapper             batch.Wrapper[V, R]
	Sizer               batch.Sizer[V]
	Inserter            func(clientID string, batch R) (middleware.Message, error)
	processor           processor.StatefulProcessor[T, V]
}

func buildStatefulWorkerInputExchangeOutputQueue[T, V, R any](cfg InputExchangeOutputQueueConfig[T, V, R]) (workers.Worker, error) {
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
		cfg.ReceivedMessageType,
		cfg.Extractor,
	)

	sender := sender.NewSingleSender(
		outputQueue,
		cfg.Wrapper,
		cfg.Sizer,
		maxBatchWeight,
		cfg.Inserter,
	)

	engine := engine.NewStatefulEngine(receiver, sender, cfg.processor, newCoordinator)
	worker := worker.NewWorker()
	worker.AddEngine(engine)
	return worker, nil
}
