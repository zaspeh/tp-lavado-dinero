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
	"google.golang.org/protobuf/proto"
)

type GroupByOriginWorker struct {
	inputExchange  *middleware.ExchangeMiddleware
	outputQueue    middleware.Middleware
	originsStores  map[string]*AccountStore
	storesMu       sync.RWMutex
	maxBatchWeight int
}

type GroupByOriginWorkerConfig struct {
	ID                string
	MomHost           string
	MomPort           int
	InputExchangeName string
	OutputQueueName   string
	MaxBatchWeight    int
}

func NewGroupByOriginWorker(config GroupByOriginWorkerConfig) (*GroupByOriginWorker, error) {
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

	return &GroupByOriginWorker{
		inputExchange:  inputExchange,
		outputQueue:    outputQueue,
		originsStores:  make(map[string]*AccountStore),
		maxBatchWeight: config.MaxBatchWeight,
	}, nil
}

func (gbow *GroupByOriginWorker) Run() error {
	go gbow.handleSignals()

	gbow.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gbow.handleMessage(msg, ack, nack)
	})

	return nil
}

func (gbow *GroupByOriginWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	gbow.inputExchange.Close()
	gbow.outputQueue.Close()
}

func (gbow *GroupByOriginWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SCATTERGATHER:
		gbow.handleScatterGatherMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		slog.Debug("EOF Received")
		gbow.handleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (gbow *GroupByOriginWorker) handleScatterGatherMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
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
	store := gbow.getStore(moneyLaundry.GetClientID())

	store.Add(origin, destination)

	ack()
}

func (gbow *GroupByOriginWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	batch := NewBatch(gbow.maxBatchWeight)
	store := gbow.getStore(moneyLaundry.GetClientID())
	data := store.GetData()
	totalGroups := 0
	for origin, destinations := range data {
		originBank := origin.GetBank()
		originAccount := origin.GetAccount()

		if len(destinations) < 5 {
			continue
		}

		totalGroups++

		group := &protobuf.GroupedAccounts{
			BaseAccount: &protobuf.Account{
				Bank:    originBank,
				Account: originAccount,
			},
		}

		for destination := range destinations {

			group.RelatedAccounts = append(group.RelatedAccounts, &protobuf.Account{
				Bank:    destination.GetBank(),
				Account: destination.GetAccount(),
			})
		}

		if batch.IsFull(group) {
			serializedMsg, err := serializer.SerializeProtoMessageWithClientID(moneyLaundry.GetClientID(), batch.Get(), protobuf.MessageType_GROUPED_ACCOUNTS_BATCH)
			if err != nil {
				nack()
				return
			}

			if err := gbow.outputQueue.Send(*serializedMsg); err != nil {
				nack()
				return
			}
		}

		if !batch.Add(group) {
			slog.Debug("Sending NACK",
				"size of group",
				proto.Size(group))
			nack()
			return
		}
	}

	if !batch.IsEmpty() {
		serializedMsg, err := serializer.SerializeProtoMessageWithClientID(moneyLaundry.GetClientID(), batch.Get(), protobuf.MessageType_GROUPED_ACCOUNTS_BATCH)
		if err != nil {
			nack()
			return
		}

		if err := gbow.outputQueue.Send(*serializedMsg); err != nil {
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

	if err := gbow.outputQueue.Send(eofMsg); err != nil {
		nack()
		return
	}

	gbow.storesMu.Lock()
	delete(
		gbow.originsStores,
		moneyLaundry.GetClientID(),
	)
	gbow.storesMu.Unlock()

	ack()
}

func (gbow *GroupByOriginWorker) getStore(
	clientID string,
) *AccountStore {

	gbow.storesMu.Lock()
	defer gbow.storesMu.Unlock()

	store, ok := gbow.originsStores[clientID]

	if !ok {
		store = newAccountStore()
		gbow.originsStores[clientID] = store
	}

	return store
}
