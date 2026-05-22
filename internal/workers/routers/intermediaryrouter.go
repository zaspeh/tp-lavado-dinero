package routers

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type IntermediaryRouter struct {
	inputQueue                          middleware.Middleware
	aggregateByIntermediaryExchange     *middleware.ExchangeMiddleware
	aggregateByIntermediaryWorkers      int
	aggregateByIntermediaryExchangeKeys []string
}

type IntermediaryRouterConfig struct {
	InputQueueName                string
	AggregateByIntermediaryName   string
	AggregateByIntermediaryAmount int
	MomHost                       string
	MomPort                       int
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

	return &IntermediaryRouter{
		inputQueue:                          inputQueue,
		aggregateByIntermediaryExchange:     aggregateByIntermediaryExchange,
		aggregateByIntermediaryWorkers:      config.AggregateByIntermediaryAmount,
		aggregateByIntermediaryExchangeKeys: aggregateByIntermediaryKeys,
	}, nil
}

func (ir *IntermediaryRouter) Run() error {
	go ir.handleSignals()

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
		ir.handleEOF(moneyLaundry, ack, nack)
	default:
		nack()
	}
}

func (ir *IntermediaryRouter) handleBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	/*batch, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.GroupedAccountsBatch{})
	if err != nil {
		nack()
		return
	}

	for _, group := range batch.GetGroups() {

	*/
}

func (ir *IntermediaryRouter) handleEOF(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	//TODO
}
