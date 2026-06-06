package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
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
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		config.Sender.Close()
		return nil, err
	}

	engine, err := e.NewStatelessEngine(receiver, config.Sender, config.Processor, coordinator)
	if err != nil {
		inputQueue.Close()
		config.Sender.Close()
		return nil, err
	}

	worker := worker.NewWorker()
	worker.AddEngine(engine)
	return worker, nil
}

type InputQueueOutputQueueStatelessConfig[T, V, R any] struct {
	ReceivedMessageType protobuf.MessageType
	Extractor           func(*protobuf.MoneyLaundry) []T
	Wrapper             batch.Wrapper[V, R]
	Sizer               batch.Sizer[V]
	Inserter            func(clientID string, batch R) (middleware.Message, error)
	processor           processor.Processor[T, V]
}

func buildStatefulWorkerInputQueueOutputQueue[T, V, R any](cfg InputQueueOutputQueueStatelessConfig[T, V, R]) (workers.Worker, error) {
	inputQueue, outputQueue, err := createInputOutputQueues()
	if err != nil {
		return nil, err
	}

	coordinator, err := getCoordinator()
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	singleSender := s.NewSingleSender(
		outputQueue,
		cfg.Wrapper,
		cfg.Sizer,
		0,
		cfg.Inserter,
	)

	singleReceiver := r.NewSingleReceiver(
		inputQueue,
		cfg.ReceivedMessageType,
		cfg.Extractor,
	)

	engineInstance, err := engine.NewStatelessEngine(
		singleReceiver,
		singleSender,
		cfg.processor,
		coordinator,
	)
	if err != nil {
		singleSender.Close()
		singleReceiver.Close()
		coordinator.Close()
		return nil, err
	}

	workerInstance := worker.NewWorker()
	workerInstance.AddEngine(engineInstance)

	return workerInstance, nil
}
