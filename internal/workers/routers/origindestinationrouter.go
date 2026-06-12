package routers

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
)

type OriginDestinationRouter struct {
	inputQueue                middleware.Middleware
	groupByOriginWorkers      int
	groupByOriginExchangeKeys []string

	groupByExchange     *middleware.ExchangeMiddleware
	groupByExchangeKeys []string

	groupByDestinationWorkers      int
	groupByDestinationExchangeKeys []string
	coordinator                    *c.EOFCoordinator
}

type OriginDestinationRouterConfig struct {
	ID                              int
	InputQueueName                  string
	GroupByOriginWorkersAmount      int
	GroupByDestinationWorkersAmount int
	GroupByExchangeName             string
	MomHost                         string
	MomPort                         int
	WorkerCount                     int
	WorkerExchangeName              string
}

func NewOriginDestinationRouter(config OriginDestinationRouterConfig) (*OriginDestinationRouter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	keys := []string{}

	for i := 0; i < config.GroupByOriginWorkersAmount; i++ {
		keys = append(keys, fmt.Sprintf("origin.%d", i))
	}

	for i := 0; i < config.GroupByDestinationWorkersAmount; i++ {
		keys = append(keys, fmt.Sprintf("destination.%d", i))
	}

	groupByExchange, err := middleware.CreateExchangeMiddleware(config.GroupByExchangeName, keys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	originDestinationRouter := &OriginDestinationRouter{
		inputQueue:                     inputQueue,
		groupByOriginWorkers:           config.GroupByOriginWorkersAmount,
		groupByDestinationWorkers:      config.GroupByDestinationWorkersAmount,
		groupByOriginExchangeKeys:      keys[:config.GroupByOriginWorkersAmount],
		groupByDestinationExchangeKeys: keys[config.GroupByOriginWorkersAmount:],
		groupByExchange:                groupByExchange,
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.ID,
		WorkerCount:       config.WorkerCount,
		ExpectedEOFs:      0,
		FlushHandler:      originDestinationRouter.handleFlush,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		groupByExchange.Close()
		return nil, err
	}

	originDestinationRouter.coordinator = coordinator

	return originDestinationRouter, nil
}

func (odr *OriginDestinationRouter) Run() error {
	slog.Debug("SStarting origin destination router")

	go odr.handleSignals()
	go odr.coordinator.Run()

	odr.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		odr.handleMessage(msg, ack, nack)
	})

	return nil
}

func (odr *OriginDestinationRouter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	odr.inputQueue.Close()

	odr.groupByExchange.Close()

	odr.coordinator.Close()
}

func (odr *OriginDestinationRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SCATTERGATHER_BATCH:
		slog.Debug("Message ScatterGather Received")
		odr.handleScatterGatherMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		slog.Debug("EOF received, broadcasting to all groupers")
		odr.handleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (odr *OriginDestinationRouter) handleScatterGatherMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	scatterGatherBatch := moneyLaundry.GetScattergatherBatch()

	if scatterGatherBatch == nil || len(scatterGatherBatch.GetItems()) == 0 {
		ack()
		return
	}

	originBatchesByKey := make(map[string][]*protobuf.ScatterGather)
	destinationBatchesByKey := make(map[string][]*protobuf.ScatterGather)
	for _, scatterGatherMessage := range scatterGatherBatch.GetItems() {
		origin := scatterGatherMessage.GetFromBank()
		account := scatterGatherMessage.GetAccount()

		workerKey := odr.selectOriginWorker(origin, account)
		originBatchesByKey[workerKey] = append(originBatchesByKey[workerKey], scatterGatherMessage)
	}

	for _, scatterGatherMessage := range scatterGatherBatch.GetItems() {
		destination := scatterGatherMessage.GetToBank()
		account := scatterGatherMessage.GetToAccount()

		workerKey := odr.selectDestinationWorker(destination, account)
		destinationBatchesByKey[workerKey] = append(destinationBatchesByKey[workerKey], scatterGatherMessage)
	}

	if err := odr.publishToGroupByOrigin(originBatchesByKey, clientID); err != nil {
		nack()
		return
	}

	if err := odr.publishToGroupByDestination(destinationBatchesByKey, clientID); err != nil {
		nack()
		return
	}

	for _, _ = range scatterGatherBatch.GetItems() {
		if err := odr.coordinator.RecordProcessed(clientID); err != nil {
			nack()
			return
		}

		if err := odr.coordinator.RecordSurvivor(clientID); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (odr *OriginDestinationRouter) publishToGroupByOrigin(originBatchesByKey map[string][]*protobuf.ScatterGather, clientID string) error {
	for workerKey, batches := range originBatchesByKey {
		innerMessage := &protobuf.MoneyLaundry_ScattergatherBatch{
			ScattergatherBatch: &protobuf.ScatterGatherBatch{
				Items: batches,
			},
		}

		msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_SCATTERGATHER_BATCH, innerMessage, "")
		if err != nil {
			return err
		}

		if err := odr.groupByExchange.SendWithKey(workerKey, msg); err != nil {
			return err
		}
	}
	return nil
}

func (odr *OriginDestinationRouter) publishToGroupByDestination(destinationBatchesByKey map[string][]*protobuf.ScatterGather, clientID string) error {
	for workerKey, batches := range destinationBatchesByKey {
		innerMessage := &protobuf.MoneyLaundry_ScattergatherBatch{
			ScattergatherBatch: &protobuf.ScatterGatherBatch{
				Items: batches,
			},
		}

		msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_SCATTERGATHER_BATCH, innerMessage, "")
		if err != nil {
			return err
		}

		if err := odr.groupByExchange.SendWithKey(workerKey, msg); err != nil {
			return err
		}
	}
	return nil
}

func (odr *OriginDestinationRouter) selectOriginWorker(bank int32, account string) string {
	hash := odr.hash(bank, account)
	return odr.groupByOriginExchangeKeys[hash%uint32(odr.groupByOriginWorkers)]
}

func (odr *OriginDestinationRouter) selectDestinationWorker(bank int32, account string) string {
	hash := odr.hash(bank, account)
	return odr.groupByDestinationExchangeKeys[hash%uint32(odr.groupByDestinationWorkers)]
}

func (odr *OriginDestinationRouter) hash(bank int32, account string) uint32 {
	h := fnv.New32a()

	h.Write([]byte(fmt.Sprintf("%d-%s", bank, account)))

	return h.Sum32()
}

func (odr *OriginDestinationRouter) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	eofMessage := moneyLaundry.GetEofMessage()
	if err := odr.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (odr *OriginDestinationRouter) handleFlush(clientID string, totalSurvivors uint64) error {
	slog.Debug("Checking if im leader")
	if !odr.coordinator.IsLeader() {
		return nil
	}

	slog.Info("Flushing client", "clientID", clientID, "totalSurvivors", totalSurvivors)
	innerMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: totalSurvivors,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, innerMessage, "")
	if err != nil {
		return err
	}

	for _, key := range odr.groupByOriginExchangeKeys {
		slog.Debug("sending EOF to Client")
		if err := odr.groupByExchange.SendWithKey(key, msg); err != nil {
			return err
		}
	}

	for _, key := range odr.groupByDestinationExchangeKeys {
		slog.Debug("sending EOF to Client")
		if err := odr.groupByExchange.SendWithKey(key, msg); err != nil {
			return err
		}
	}

	return nil
}
