package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
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

	id, _, namespace, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	inputExchange, outputQueue, err := createInputExchangeOutputQueue(cfg.keys)
	if err != nil {
		return nil, err
	}

	newCoordinator, err := getCoordinator(maxBatchWeight, 1)
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		return nil, err
	}

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
		namespace,
	)

	cm, err := getCheckpointManager(cfg.processor.(checkpoint.Checkpointable))
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		newCoordinator.Close()
		return nil, err
	}

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		inputExchange.Close()
		outputQueue.Close()
		newCoordinator.Close()
		return nil, err
	}

	engine := engine.NewStatefulEngine(id, receiver, sender, cfg.processor, newCoordinator, cm)
	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}
