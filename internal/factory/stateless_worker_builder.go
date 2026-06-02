package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

type statelessWorkerConfig[T, V, B any] struct {
	Mom                  m.ConnSettings
	id                   int
	workerCount          int
	workerExchangeName   string
	InputQueueName       string
	OutputQueueName      string
	InputMessageType     protobuf.MessageType
	ExtractInputItems    func(*protobuf.MoneyLaundry) []T
	Processor            p.Processor[T, V]
	OutputWrapper        batch.Wrapper[V, B]
	OutputSizer          batch.Sizer[V]
	SerializeOutputBatch func(clientID string, batch B) (m.Message, error)
}

func buildStatelessWorker[T, V, B any](config statelessWorkerConfig[T, V, B]) (*worker.Worker, error) {
	inputQueue, err := m.CreateQueueMiddleware(config.InputQueueName, config.Mom)
	if err != nil {
		return nil, err
	}

	outputQueue, err := m.CreateQueueMiddleware(config.OutputQueueName, config.Mom)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	receiver := r.NewSingleReceiver(
		inputQueue,
		config.InputMessageType,
		config.ExtractInputItems,
	)
	sender := s.NewSingleSender(
		outputQueue,
		config.OutputWrapper,
		config.OutputSizer,
		0,
		config.SerializeOutputBatch,
	)

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.workerExchangeName,
		ConnSettings:      config.Mom,
		WorkerID:          config.id,
		WorkerCount:       config.workerCount,
	}

	engine, err := e.NewStatelessEngine(receiver, sender, config.Processor, coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	worker := worker.NewWorker()
	worker.AddEngine(engine)
	return worker, nil
}
