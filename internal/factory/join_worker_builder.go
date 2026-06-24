package factory

import (
	"fmt"
	"os"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
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
// T es el item individual que recibe el processor (e.g., *protobuf.MaxBankResult).
// V es el resultado individual del processor (e.g., *protobuf.MaxBankResult).
// R es el batch que arma el sender antes de publicarlo.
type joinWorkerConfig[T, V, R any] struct {
	Mom            middleware.ConnSettings
	ID             int
	InputExchange  string
	ClientExchange string
	MaxBatchWeight int
	ReceivedType   protobuf.MessageType
	ExtractItems   func(*protobuf.MoneyLaundry) []T
	Processor      processor.StatefulProcessor[T, V]
	Wrapper        batch.Wrapper[V, R]
	Sizer          batch.Sizer[V]
	Inserter       sender.SerializerFunc[R]
}

func buildJoinWorker[T, V, R any](cfg joinWorkerConfig[T, V, R]) (workers.Worker, error) {
	inputExchangeKeys := []string{
		fmt.Sprintf("%s.%s", cfg.InputExchange, strconv.Itoa(cfg.ID)),
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(cfg.InputExchange, inputExchangeKeys, cfg.Mom, false, false, strconv.Itoa(cfg.ID), os.Getenv("WORKER_TYPE"))
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(cfg.ClientExchange, []string{cfg.ClientExchange}, cfg.Mom, false, false, strconv.Itoa(cfg.ID), os.Getenv("WORKER_TYPE"))
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	newCoordinator, err := getCoordinator(cfg.MaxBatchWeight, 1)
	if err != nil {
		inputExchange.Close()
		resultExchange.Close()
		return nil, err
	}

	recv := receiver.NewSingleReceiver(inputExchange, cfg.ReceivedType, cfg.ExtractItems)

	cm, err := getCheckpointManager(cfg.Processor.(checkpoint.Checkpointable))
	if err != nil {
		inputExchange.Close()
		resultExchange.Close()
		newCoordinator.Close()
		return nil, err
	}

	dynSender := sender.NewDynamicKeySender(
		resultExchange,
		func(clientID string) string {
			return fmt.Sprintf(
				"%s.%s",
				cfg.ClientExchange,
				clientID,
			)
		},
		cfg.Wrapper,
		cfg.Sizer,
		cfg.MaxBatchWeight,
		cfg.Inserter,
		cfg.InputExchange, // TODO: cuando se use coordinador, usar ese exchange.
	)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		inputExchange.Close()
		resultExchange.Close()
		newCoordinator.Close()
		return nil, err
	}

	eng := engine.NewStatefulEngine(cfg.ID, recv, dynSender, cfg.Processor, newCoordinator, cm)
	w := worker.NewWorker(heartbeatPublisher)
	w.AddEngine(eng)
	return w, nil
}
