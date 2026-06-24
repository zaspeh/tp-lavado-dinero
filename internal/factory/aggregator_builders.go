package factory

import (
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/aggregators/aggregatebyintermediary"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

func buildAggregateByIntermediaryWorker() (workers.Worker, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	originInputExchangeName, err := getEnvStrict("ORIGIN_INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}
	originInputExchangeKeys := []string{originInputExchangeName + "." + strconv.Itoa(id)}
	originInputExchange, err := middleware.CreateExchangeMiddleware(originInputExchangeName, originInputExchangeKeys, mom, false, false, strconv.Itoa(id))
	if err != nil {
		return nil, err
	}

	destinationInputExchangeName, err := getEnvStrict("DESTINATION_INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}
	destinationInputExchangeKeys := []string{destinationInputExchangeName + "." + strconv.Itoa(id)}
	destinationInputExchange, err := middleware.CreateExchangeMiddleware(destinationInputExchangeName, destinationInputExchangeKeys, mom, false, false, strconv.Itoa(id))
	if err != nil {
		originInputExchange.Close()
		return nil, err
	}

	outputQueueName, err := getEnvStrict("OUTPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}
	outputQueue, err := middleware.CreateQueueMiddleware(outputQueueName, mom)
	if err != nil {
		originInputExchange.Close()
		destinationInputExchange.Close()
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	newCoordinator := coordinator.NewMultiEOFCoordinator(id, 2)

	inputsIDs := []string{"origin", "destination"}

	receiver := receiver.NewFanInReceiver([]middleware.Middleware{originInputExchange, destinationInputExchange}, inputsIDs, protobuf.MessageType_INTERMEDIARYPAIR_BATCH, aggregatebyintermediary.GetIntermediaryPairBatchItems)

	// TODO: Usar el excahnge name del coordinator
	sender := sender.NewSingleSender(outputQueue, protowrappers.WrapSuspiciousPaths, protowrappers.ProtoSizer[*protobuf.SuspiciousPath](), maxBatchWeight, protoinserters.InsertSuspiciousPathBatch, outputQueueName)

	processor := aggregatebyintermediary.NewAggregateByIntermediaryProcessor()

	cm, err := getCheckpointManager(processor)
	if err != nil {
		return nil, err
	}

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		return nil, err
	}

	engine := engine.NewStatefulEngine(receiver, sender, processor, newCoordinator, cm)
	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}
