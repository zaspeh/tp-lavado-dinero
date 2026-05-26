package routers

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type OriginDestinationRouter struct {
	inputQueue                middleware.Middleware
	groupByOriginExchange     *middleware.ExchangeMiddleware
	groupByOriginWorkers      int
	groupByOriginExchangeKeys []string

	groupByDestinationExchange     *middleware.ExchangeMiddleware
	groupByDestinationWorkers      int
	groupByDestinationExchangeKeys []string
	coordinator                    *c.EOFCoordinator
}

type OriginDestinationRouterConfig struct {
	ID                              int
	InputQueueName                  string
	GroupByOriginExchangeName       string
	GroupByDestinationExchangeName  string
	GroupByOriginWorkersAmount      int
	GroupByDestinationWorkersAmount int
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

	groupByOriginKeys := make([]string, config.GroupByOriginWorkersAmount)
	for i := range groupByOriginKeys {
		groupByOriginKeys[i] = fmt.Sprintf("%s.%d", config.GroupByOriginExchangeName, i)
	}

	groupByDestinationKeys := make([]string, config.GroupByDestinationWorkersAmount)
	for i := range groupByDestinationKeys {
		groupByDestinationKeys[i] = fmt.Sprintf("%s.%d", config.GroupByDestinationExchangeName, i)
	}

	groupByOriginExchange, err := middleware.CreateExchangeMiddleware(config.GroupByOriginExchangeName, groupByOriginKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	groupByDestinationExchange, err := middleware.CreateExchangeMiddleware(config.GroupByDestinationExchangeName, groupByDestinationKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		groupByOriginExchange.Close()
		return nil, err
	}

	originDestinationRouter := &OriginDestinationRouter{
		inputQueue:                     inputQueue,
		groupByOriginExchange:          groupByOriginExchange,
		groupByDestinationExchange:     groupByDestinationExchange,
		groupByOriginWorkers:           config.GroupByOriginWorkersAmount,
		groupByDestinationWorkers:      config.GroupByDestinationWorkersAmount,
		groupByOriginExchangeKeys:      groupByOriginKeys,
		groupByDestinationExchangeKeys: groupByDestinationKeys,
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
		groupByOriginExchange.Close()
		groupByDestinationExchange.Close()
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

	odr.groupByOriginExchange.Close()
	odr.groupByDestinationExchange.Close()

	odr.coordinator.Close()
}

func (odr *OriginDestinationRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SCATTERGATHER:
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
	scatterGatherMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.ScatterGather{})
	if err != nil {
		nack()
		return
	}

	if err := odr.publishToGroupByOrigin(scatterGatherMsg, msg); err != nil {
		nack()
		return
	}

	if err := odr.publishToGroupByDestination(scatterGatherMsg, msg); err != nil {
		nack()
		return
	}

	if err := odr.coordinator.RecordProcessed(clientID); err != nil {
		nack()
		return
	}

	if err := odr.coordinator.RecordSurvivor(clientID); err != nil {
		nack()
		return
	}

	ack()
}

func (odr *OriginDestinationRouter) publishToGroupByOrigin(scatterGatherMsg *protobuf.ScatterGather, msg middleware.Message) error {
	originBank := scatterGatherMsg.GetFromBank()
	originAccount := scatterGatherMsg.GetAccount()
	workerKey := odr.selectOriginWorker(originBank, originAccount)
	return odr.groupByOriginExchange.SendWithKey(workerKey, msg)
}

func (odr *OriginDestinationRouter) publishToGroupByDestination(scatterGatherMsg *protobuf.ScatterGather, msg middleware.Message) error {
	destinationBank := scatterGatherMsg.GetToBank()
	destinationAccount := scatterGatherMsg.GetToAccount()
	workerKey := odr.selectDestinationWorker(destinationBank, destinationAccount)
	return odr.groupByDestinationExchange.SendWithKey(workerKey, msg)
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

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, innerMessage)
	if err != nil {
		return err
	}

	for _, key := range odr.groupByOriginExchangeKeys {
		slog.Debug("sending EOF to Client")
		if err := odr.groupByOriginExchange.SendWithKey(key, msg); err != nil {
			return err
		}
	}

	for _, key := range odr.groupByDestinationExchangeKeys {
		slog.Debug("sending EOF to Client")
		if err := odr.groupByDestinationExchange.SendWithKey(key, msg); err != nil {
			return err
		}
	}

	return nil
}
