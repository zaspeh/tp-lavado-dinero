package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

type statelessWorkerWithSenderConfig[T, V any] struct {
	Mom                m.ConnSettings
	id                 int
	workerCount        int
	workerExchangeName string
	expectedEOFs       int
	InputQueueName     string
	InputMessageType   protobuf.MessageType
	ExtractInputItems  func(*protobuf.MoneyLaundry) []T
	Processor          p.Processor[T, V]
	Sender             s.Sender[V]
	maxBatchWeight     int
}

func buildStatelessWorkerWithSender[T, V any](config statelessWorkerWithSenderConfig[T, V]) (*worker.Worker, error) {
	inputQueue, err := m.CreateQueueMiddleware(config.InputQueueName, config.Mom)
	if err != nil {
		config.Sender.Close()
		return nil, err
	}

	receiver := r.NewSingleReceiver(
		inputQueue,
		config.InputMessageType,
		config.ExtractInputItems,
	)

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.workerExchangeName,
		ConnSettings:      config.Mom,
		WorkerID:          config.id,
		WorkerCount:       config.workerCount,
		ExpectedEOFs:      config.expectedEOFs,
		MaxBatchWeight:    config.maxBatchWeight,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		config.Sender.Close()
		return nil, err
	}

	engine := e.NewStatelessEngine(receiver, config.Sender, config.Processor, coordinator)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		engine.Shutdown()
	}

	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}

type InputQueueOutputQueueStatelessConfig[T, V, R any] struct {
	ReceivedMessageType protobuf.MessageType
	Extractor           func(*protobuf.MoneyLaundry) []T
	Wrapper             batch.Wrapper[V, R]
	Sizer               batch.Sizer[V]
	Inserter            sender.SerializerFunc[R]
	Processor           processor.Processor[T, V]
	MaxBatchWeight      int
}

func buildStatelessWorkerInputQueueOutputQueue[T, V, R any](cfg InputQueueOutputQueueStatelessConfig[T, V, R]) (workers.Worker, error) {
	inputQueue, outputQueue, err := createInputOutputQueues()
	if err != nil {
		return nil, err
	}

	_, _, namespace, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	coordinator, err := getCoordinator(cfg.MaxBatchWeight)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	singleSender := s.NewSingleSender(
		outputQueue,
		cfg.Wrapper,
		cfg.Sizer,
		cfg.MaxBatchWeight,
		cfg.Inserter,
		namespace,
	)

	singleReceiver := r.NewSingleReceiver(
		inputQueue,
		cfg.ReceivedMessageType,
		cfg.Extractor,
	)

	engineInstance := engine.NewStatelessEngine(
		singleReceiver,
		singleSender,
		cfg.Processor,
		coordinator,
	)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		engineInstance.Shutdown()
		return nil, err
	}

	workerInstance := worker.NewWorker(heartbeatPublisher)
	workerInstance.AddEngine(engineInstance)

	return workerInstance, nil
}
