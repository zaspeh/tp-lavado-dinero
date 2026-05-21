package origin

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type GroupByOriginWorker struct {
	inputExchange  *middleware.ExchangeMiddleware
	outputQueue    middleware.Middleware
	originsStore   *AccountStore
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
		originsStore:   newAccountStore(),
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

	gbow.originsStore.Add(origin, destination)

	ack()
}

func (gbow *GroupByOriginWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	/*for origin, destinations := range gbow.originsStore.GetData() {
	originBank := origin.GetBank()
	originAccount := origin.GetAccount()

	if len(destinations) < 5 {
		continue
	}

	for _, destination := range destinations {
		scatterGatherMsg := &protobuf.ScatterGather{
			FromBank:   originBank,
			Account:    originAccount,
			ToBank:     destination.GetBank(),
			ToAccount:  destination.GetAccount(),
		}

		gbow.outputQueue.Send(serializer.SerializeProtoMessage(scatterGatherMsg, protobuf.MessageType_ESCATTERGATHER))*/
}
