package origindestination

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type GroupByDestinationWorker struct {
	inputExchange      *middleware.ExchangeMiddleware
	outputQueue        middleware.Middleware
	destinationsStores map[string]*AccountStore
	storesMu           sync.RWMutex
	maxBatchWeight     int
}

type GroupByDestinationWorkerConfig struct {
	ID                string
	MomHost           string
	MomPort           int
	InputExchangeName string
	OutputQueueName   string
	MaxBatchWeight    int
}

func NewGroupByDestinationWorker(config GroupByDestinationWorkerConfig) (*GroupByDestinationWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputExchangeKeys := []string{config.InputExchangeName + "." + config.ID}
	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, inputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &GroupByDestinationWorker{
		inputExchange:      inputExchange,
		outputQueue:        outputQueue,
		destinationsStores: make(map[string]*AccountStore),
		maxBatchWeight:     config.MaxBatchWeight,
	}, nil
}

func (gbdw *GroupByDestinationWorker) Run() error {
	go gbdw.handleSignals()

	gbdw.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gbdw.handleMessage(msg, ack, nack)
	})

	return nil
}

func (gbdw *GroupByDestinationWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	gbdw.inputExchange.Close()
	gbdw.outputQueue.Close()
}

func (gbdw *GroupByDestinationWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SCATTERGATHER:
		gbdw.handleScatterGatherMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		slog.Debug("EOF received")
		gbdw.handleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (gbdw *GroupByDestinationWorker) handleScatterGatherMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	scatterGatherMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.ScatterGather{})
	if err != nil {
		nack()
		return
	}

	origin := Account{
		Bank:    scatterGatherMsg.GetFromBank(),
		Account: scatterGatherMsg.GetAccount(),
	}

	destination := Account{
		Bank:    scatterGatherMsg.GetToBank(),
		Account: scatterGatherMsg.GetToAccount(),
	}

	store := gbdw.getStore(moneyLaundry.GetClientID())

	store.Add(destination, origin)

	ack()
}

func (gbdw *GroupByDestinationWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	batch := NewBatch(gbdw.maxBatchWeight)
	slog.Debug("Creating batch")
	store := gbdw.getStore(moneyLaundry.GetClientID())
	data := store.GetData()
	totalGroups := 0

	for destination, origins := range data {
		destinationBank := destination.GetBank()
		destinationAccount := destination.GetAccount()
		slog.Debug("Analizing destination")
		if len(origins) < 5 {
			slog.Debug("Destination with less than five origins, discarding")
			continue
		}
		totalGroups++
		slog.Debug("Destination with more than five origins, adding to batch")

		group := &protobuf.GroupedAccounts{
			BaseAccount: &protobuf.Account{
				Bank:    destinationBank,
				Account: destinationAccount,
			},
		}

		slog.Debug("Adding origins to destination")
		for origin := range origins {

			group.RelatedAccounts = append(group.RelatedAccounts, &protobuf.Account{
				Bank:    origin.GetBank(),
				Account: origin.GetAccount(),
			})
		}

		if batch.IsFull(group) {
			slog.Debug("Batch is full, serializing message")
			serializedMsg, err := serializer.SerializeProtoMessageWithClientID(moneyLaundry.GetClientID(), batch.Get(), protobuf.MessageType_GROUPED_ACCOUNTS_BATCH)
			if err != nil {
				nack()
				return
			}
			slog.Debug("Batch is full, sending batch")
			if err := gbdw.outputQueue.Send(*serializedMsg); err != nil {
				nack()
				return
			}
		}

		slog.Debug("Batch NOT full, adding group")
		if !batch.Add(group) {
			nack()
			return
		}
	}

	slog.Debug("Batch NOT empty after adding all groups")
	if !batch.IsEmpty() {
		slog.Debug("serializing Batch")
		serializedMsg, err := serializer.SerializeProtoMessageWithClientID(moneyLaundry.GetClientID(), batch.Get(), protobuf.MessageType_GROUPED_ACCOUNTS_BATCH)
		if err != nil {
			nack()
			return
		}

		slog.Debug("sending Batch")
		if err := gbdw.outputQueue.Send(*serializedMsg); err != nil {
			nack()
			return
		}
	}

	slog.Debug("Creating new EOF")

	innerMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: uint64(totalGroups),
		},
	}

	eofMsg, err := protobuf.SerializeProtoMessageONTRIAL(moneyLaundry.GetClientID(), protobuf.MessageType_EOF_, innerMessage)
	if err != nil {
		nack()
		return
	}

	slog.Debug(
		"Forwarding EOF",
		"groups",
		totalGroups,
	)

	if err := gbdw.outputQueue.Send(eofMsg); err != nil {
		nack()
		return
	}

	gbdw.storesMu.Lock()
	delete(
		gbdw.destinationsStores,
		moneyLaundry.GetClientID(),
	)
	gbdw.storesMu.Unlock()

	slog.Debug("ack eof message")
	ack()
}

func (gbdw *GroupByDestinationWorker) getStore(
	clientID string,
) *AccountStore {

	gbdw.storesMu.Lock()
	defer gbdw.storesMu.Unlock()

	store, ok := gbdw.destinationsStores[clientID]

	if !ok {
		store = newAccountStore()
		gbdw.destinationsStores[clientID] = store
	}

	return store
}
