package origindestination

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type GroupByDestinationWorker struct {
	inputExchange     *middleware.ExchangeMiddleware
	outputQueue       middleware.Middleware
	destinationsStore *AccountStore
	maxBatchWeight    int
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
		inputExchange:     inputExchange,
		outputQueue:       outputQueue,
		destinationsStore: newAccountStore(),
		maxBatchWeight:    config.MaxBatchWeight,
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

	gbdw.destinationsStore.Add(destination, origin)

	ack()
}

func (gbdw *GroupByDestinationWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	// TODO: IMPLEMENTAR LÓGICA DE ENVÍO DE LOS LOTES AL SIGUIENTE WORKER
}
