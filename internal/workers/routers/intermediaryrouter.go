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

type IntermediaryRouter struct {
	inputQueue                          middleware.Middleware
	aggregateByIntermediaryExchange     *middleware.ExchangeMiddleware
	aggregateByIntermediaryWorkers      int
	aggregateByIntermediaryExchangeKeys []string
	coordinator                         *c.EOFCoordinator
}

type IntermediaryRouterConfig struct {
	ID                            int
	InputQueueName                string
	AggregateByIntermediaryName   string
	AggregateByIntermediaryAmount int
	MomHost                       string
	MomPort                       int
	WorkerCount                   int
	WorkerExchangeName            string
}

func NewIntermediaryRouter(config IntermediaryRouterConfig) (*IntermediaryRouter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryKeys := make([]string, config.AggregateByIntermediaryAmount)
	for i := range aggregateByIntermediaryKeys {
		aggregateByIntermediaryKeys[i] = fmt.Sprintf("%s.%d", config.AggregateByIntermediaryName, i)
	}

	aggregateByIntermediaryExchange, err := middleware.CreateExchangeMiddleware(config.AggregateByIntermediaryName, aggregateByIntermediaryKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	intermediaryRouter := &IntermediaryRouter{
		inputQueue:                          inputQueue,
		aggregateByIntermediaryExchange:     aggregateByIntermediaryExchange,
		aggregateByIntermediaryWorkers:      config.AggregateByIntermediaryAmount,
		aggregateByIntermediaryExchangeKeys: aggregateByIntermediaryKeys,
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.ID,
		WorkerCount:       config.WorkerCount,
		ExpectedEOFs:      2,
		FlushHandler:      intermediaryRouter.handleFlush,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)

	if err != nil {
		inputQueue.Close()
		aggregateByIntermediaryExchange.Close()
		return nil, err
	}
	intermediaryRouter.coordinator = coordinator

	return intermediaryRouter, nil
}

func (ir *IntermediaryRouter) Run() error {
	go ir.handleSignals()

	go ir.coordinator.Run()

	ir.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		ir.handleMessage(msg, ack, nack)
	})

	return nil
}

func (ir *IntermediaryRouter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	ir.inputQueue.Close()
	ir.aggregateByIntermediaryExchange.Close()
	ir.coordinator.Close()
}

func (ir *IntermediaryRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_GROUPED_ACCOUNTS_BATCH:
		ir.handleBatch(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		ir.handleEOF(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (ir *IntermediaryRouter) handleBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	slog.Debug("ClientID", "clientID", clientID)
	batch, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.GroupedAccountsBatch{})
	if err != nil {
		nack()
		return
	}

	batchesByKey := make(map[string][]*protobuf.IntermediaryPair)
	for _, group := range batch.GetGroups() {
		baseAccount := group.GetBaseAccount()

		for _, intermediary := range group.GetRelatedAccounts() {

			match := &protobuf.IntermediaryPair{
				Intermediary: intermediary,
				Account:      baseAccount,
			}

			workerKey := ir.selectWorkerKey(intermediary)

			batchesByKey[workerKey] = append(batchesByKey[workerKey], match)

			if err := ir.coordinator.RecordSurvivor(clientID); err != nil {
				nack()
				return
			}
		}
		if err := ir.coordinator.RecordProcessed(clientID); err != nil {
			nack()
			return
		}
	}

	for workerKey, batchMessages := range batchesByKey {
		innerMessage := &protobuf.MoneyLaundry_IntermediarypairBatch{
			IntermediarypairBatch: &protobuf.IntermediaryPairBatch{
				Items: batchMessages,
			},
		}

		msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_INTERMEDIARYPAIR_BATCH, innerMessage)
		if err != nil {
			nack()
			return
		}

		if err := ir.aggregateByIntermediaryExchange.SendWithKey(workerKey, msg); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (ir *IntermediaryRouter) handleEOF(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	eofMessage := moneyLaundry.GetEofMessage()
	if err := ir.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (ir *IntermediaryRouter) selectWorkerKey(intermediary *protobuf.Account) string {
	h := fnv.New32a()
	h.Write([]byte(fmt.Sprintf("%d-%s", intermediary.GetBank(), intermediary.GetAccount())))

	return ir.aggregateByIntermediaryExchangeKeys[h.Sum32()%uint32(ir.aggregateByIntermediaryWorkers)]
}

func (ir *IntermediaryRouter) handleFlush(clientID string, totalSurvivors uint64) error {
	if !ir.coordinator.IsLeader() {
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

	for _, key := range ir.aggregateByIntermediaryExchangeKeys {
		if err := ir.aggregateByIntermediaryExchange.SendWithKey(key, msg); err != nil {
			return err
		}
	}

	return nil
}
