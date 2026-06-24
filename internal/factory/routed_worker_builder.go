package factory

import (
	"fmt"
	"os"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

// routedProcessorFactory construye el router concreto a partir de las keys del exchange.
// Cada router implementa Process(clientID, item) ([]sender.RoutedItem[item], error).
// Se devuelve como factory porque las keys se generan en este helper y los routers las necesitan.
type routedProcessorFactory[T any] func(keys []string) RoutedProcessor[T]

// RoutedProcessor es la interfaz que cumplen los routers-to-join: reciben un item
// y devuelven uno o varios items ruteados.
type RoutedProcessor[T any] interface {
	Process(clientID string, item T) ([]sender.RoutedItem[T], bool, error)
}

// routedProcessorAdapter envuelve un RoutedProcessor para que cumpla la interfaz
// generica processor.Processor[T, sender.RoutedItem[T]] que consume el engine.
type routedProcessorAdapter[T any] struct {
	inner RoutedProcessor[T]
}

func (a *routedProcessorAdapter[T]) Process(clientID string, item T) ([]sender.RoutedItem[T], bool, error) {
	return a.inner.Process(clientID, item)
}

// Aclaracion de Tipos:
// T es el item individual que recibe el processor (e.g., *protobuf.Microtransaction).
// B es el batch que envuelve el item antes de enviarse por el exchange.
type routedWorkerConfig[T, B any] struct {
	InputMessageType protobuf.MessageType
	ExtractInput     func(*protobuf.MoneyLaundry) []T
	MakeProcessor    routedProcessorFactory[T]
	Wrapper          batch.Wrapper[T, B]
	Sizer            batch.Sizer[T]
	Inserter         sender.SerializerFunc[B]
}

func buildRoutedToJoinWorker[T, B any](cfg routedWorkerConfig[T, B]) (*worker.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	expectedEOFs, err := getEnvIntStrict("EXPECTED_EOF_AMOUNT")
	if err != nil {
		return nil, err
	}

	inQ, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	exchangeName, err := getEnvStrict("OUTPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	outputWorkerAmount, err := getEnvIntStrict("OUTPUT_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	keys := buildNumberedExchangeKeys(exchangeName, outputWorkerAmount)

	id, workerCount, workerExchangeName, err := getCoordinationInformationFromEnv()
	if err != nil {
		return nil, err
	}

	inputQueue, err := m.CreateQueueMiddleware(inQ, mom)
	if err != nil {
		return nil, err
	}

	exchange, err := m.CreateExchangeMiddleware(exchangeName, keys, mom, false, false, strconv.Itoa(id), os.Getenv("WORKER_TYPE"))
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	routedSender := sender.NewRoutedSender(
		exchange,
		cfg.Wrapper,
		cfg.Sizer,
		maxBatchWeight,
		cfg.Inserter,
		workerExchangeName,
	)

	receiver := r.NewSingleReceiver(
		inputQueue,
		cfg.InputMessageType,
		cfg.ExtractInput,
	)

	coordinatorConfig := coordinator.EOFCoordinatorConfig{
		PeersExchangeName: workerExchangeName,
		ConnSettings:      mom,
		WorkerID:          id,
		WorkerCount:       workerCount,
		ExpectedEOFs:      expectedEOFs,
		MaxBatchWeight:    maxBatchWeight,
	}

	coord, err := coordinator.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		routedSender.Close()
		return nil, err
	}

	processor := &routedProcessorAdapter[T]{inner: cfg.MakeProcessor(keys)}
	engine := e.NewStatelessEngine(receiver, routedSender, processor, coord)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		engine.Shutdown()
		return nil, err
	}

	w := worker.NewWorker(heartbeatPublisher)
	w.AddEngine(engine)
	return w, nil
}

func buildNumberedExchangeKeys(exchangeName string, amount int) []string {
	keys := make([]string, amount)
	for i := 0; i < amount; i++ {
		keys[i] = fmt.Sprintf("%s.%d", exchangeName, i)
	}
	return keys
}
