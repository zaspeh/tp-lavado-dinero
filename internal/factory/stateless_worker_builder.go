package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	filterprocessor "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/filters"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

type statelessWorkerConfig[T, V, B any] struct {
	Mom                  m.ConnSettings
	id                   int
	workerCount          int
	workerExchangeName   string
	expectedEOFs         int
	InputQueueName       string
	OutputQueueName      string
	InputMessageType     protobuf.MessageType
	ExtractInputItems    func(*protobuf.MoneyLaundry) []T
	Processor            p.Processor[T, V]
	OutputWrapper        batch.Wrapper[V, B]
	OutputSizer          batch.Sizer[V]
	SerializeOutputBatch func(clientID string, batch B) (m.Message, error)
}

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

func buildStatelessWorker[T, V, B any](config statelessWorkerConfig[T, V, B]) (*worker.Worker, error) {
	outputQueue, err := m.CreateQueueMiddleware(config.OutputQueueName, config.Mom)
	if err != nil {
		return nil, err
	}

	sender := s.NewSingleSender(
		outputQueue,
		config.OutputWrapper,
		config.OutputSizer,
		0,
		config.SerializeOutputBatch,
	)

	return buildStatelessWorkerWithSender(statelessWorkerWithSenderConfig[T, V]{
		Mom:                config.Mom,
		id:                 config.id,
		workerCount:        config.workerCount,
		workerExchangeName: config.workerExchangeName,
		expectedEOFs:       config.expectedEOFs,
		InputQueueName:     config.InputQueueName,
		InputMessageType:   config.InputMessageType,
		ExtractInputItems:  config.ExtractInputItems,
		Processor:          config.Processor,
		Sender:             sender,
	})
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

type AmountFilterPipelineConfig[T filterprocessor.Amountable, B any] struct {
	MessageType protobuf.MessageType
	Wrapper     batch.Wrapper[T, B]
	Extractor   func(*protobuf.MoneyLaundry) []T
	Serializer  func(clientID string, batch B) (middleware.Message, error)
	Sizer       batch.Sizer[T]
}

func buildAmountFilterWorkerGeneric[T filterprocessor.Amountable, B any](cfg AmountFilterPipelineConfig[T, B]) (workers.Worker, error) {

	amountToFilter, err := getEnvFloatStrict("AMOUNT_TO_FILTER")
	if err != nil {
		return nil, err
	}

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
		cfg.Serializer,
	)

	singleReceiver := r.NewSingleReceiver(
		inputQueue,
		cfg.MessageType,
		cfg.Extractor,
	)

	processor := filterprocessor.NewAmountFilterProcessor[T](
		amountToFilter,
	)

	engineInstance, err := engine.NewStatelessEngine(
		singleReceiver,
		singleSender,
		processor,
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
