package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
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
type InputExchangeOutputQueueStatefulConfig[T, V, R any] struct {
	ReceivedMessageType protobuf.MessageType
	Extractor           func(*protobuf.MoneyLaundry) []T
	Wrapper             batch.Wrapper[V, R]
	Sizer               batch.Sizer[V]
	Inserter            sender.SerializerFunc[R]
	processor           processor.StatefulProcessor[T, V]
	keys                string //prefijo explicito de las claves, en caso de ausencia se usa el nombre del exchange
}

func buildStatefulWorkerInputExchangeOutputQueue[T, V, R any](cfg InputExchangeOutputQueueStatefulConfig[T, V, R]) (workers.Worker, error) {
	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	workerID, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	inputExchange, outputQueue, err := createInputExchangeOutputQueue(cfg.keys)
	if err != nil {
		return nil, err
	}

	newCoordinator := coordinator.NewAloneCoordinator(workerID)

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

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		return nil, err
	}

	engine := engine.NewStatefulEngine(receiver, sender, cfg.processor, newCoordinator)
	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}
