package aggregatebyintermediary

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AggregateByIntermediaryWorker struct {
	originInputExchange      *middleware.ExchangeMiddleware
	destinationInputExchange *middleware.ExchangeMiddleware

	outputQueue middleware.Middleware

	store *IntermediaryStore
}

type AggregateByIntermediaryWorkerConfig struct {
	ID                           string
	MomHost                      string
	MomPort                      int
	OriginInputExchangeName      string
	DestinationInputExchangeName string
	OutputQueueName              string
}

func NewAggregateByIntermediaryWorker(config AggregateByIntermediaryWorkerConfig) (*AggregateByIntermediaryWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	originInputExchangeKeys := []string{config.OriginInputExchangeName + "." + config.ID}
	originInputExchange, err := middleware.CreateExchangeMiddleware(config.OriginInputExchangeName, originInputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	destinationInputExchangeKeys := []string{config.DestinationInputExchangeName + "." + config.ID}
	destinationInputExchange, err := middleware.CreateExchangeMiddleware(config.DestinationInputExchangeName, destinationInputExchangeKeys, connSettings)
	if err != nil {
		originInputExchange.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		originInputExchange.Close()
		destinationInputExchange.Close()
		return nil, err
	}

	return &AggregateByIntermediaryWorker{
		originInputExchange:      originInputExchange,
		destinationInputExchange: destinationInputExchange,
		outputQueue:              outputQueue,
		store:                    NewIntermediaryStore(),
	}, nil
}

func (abi *AggregateByIntermediaryWorker) Run() error {
	go abi.handleSignals()

	go abi.originInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		abi.handleOriginMessage(msg, ack, nack)
	})

	abi.destinationInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		abi.handleDestinationMessage(msg, ack, nack)
	})

	return nil
}

func (abi *AggregateByIntermediaryWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	abi.originInputExchange.Close()
	abi.destinationInputExchange.Close()
	abi.outputQueue.Close()
}

func (abi *AggregateByIntermediaryWorker) handleOriginMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_INTERMEDIARYPAIR:
		abi.handleIntermediaryPairMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		abi.handleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (abi *AggregateByIntermediaryWorker) handleDestinationMessage(msg middleware.Message, ack, nack func()) {
	//TODO
}

func (abi *AggregateByIntermediaryWorker) handleIntermediaryPairMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	intermediaryPairMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.IntermediaryPair{})
	if err != nil {
		nack()
		return
	}

	origin := model.Account{
		Bank:    intermediaryPairMsg.GetAccount().GetBank(),
		Account: intermediaryPairMsg.GetAccount().GetAccount(),
	}

	intermediary := model.Account{
		Bank:    intermediaryPairMsg.GetIntermediary().GetBank(),
		Account: intermediaryPairMsg.GetIntermediary().GetAccount(),
	}

	abi.store.AddOrigin(intermediary, origin)

	ack()
}

func (abi *AggregateByIntermediaryWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	//TODO
}
