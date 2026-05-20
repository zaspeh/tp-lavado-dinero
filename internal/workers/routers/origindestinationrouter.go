package routers

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type OriginDestinationRouter struct {
	inputQueue                   middleware.Middleware
	groupByOriginQueue           middleware.Middleware
	groupByDestinationQueue      middleware.Middleware
	maxGroupByOriginWorkers      int
	maxGroupByDestinationWorkers int
}

type OriginDestinationRouterConfig struct {
	InputQueueName               string
	GroupByOriginQueueName       string
	GroupByDestinationQueueName  string
	MaxGroupByOriginWorkers      int
	MaxGroupByDestinationWorkers int
}

func NewOriginDestinationRouter(config OriginDestinationRouterConfig, connSettings middleware.ConnSettings) (*OriginDestinationRouter, error) {
	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	groupByOriginQueue, err := middleware.CreateQueueMiddleware(config.GroupByOriginQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	groupByDestinationQueue, err := middleware.CreateQueueMiddleware(config.GroupByDestinationQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	return &OriginDestinationRouter{
		inputQueue:                   inputQueue,
		groupByOriginQueue:           groupByOriginQueue,
		groupByDestinationQueue:      groupByDestinationQueue,
		maxGroupByOriginWorkers:      config.MaxGroupByOriginWorkers,
		maxGroupByDestinationWorkers: config.MaxGroupByDestinationWorkers,
	}, nil
}

func (pf *OriginDestinationRouter) Run() error {
	go pf.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		pf.handleMessage(msg, ack, nack)
	})

	go pf.handleSignals()

	return nil
}

func (pf *OriginDestinationRouter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	pf.inputQueue.Close()

	pf.groupByOriginQueue.Close()
	pf.groupByDestinationQueue.Close()
}

func (pf *OriginDestinationRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SCATTERGATHER:
		pf.handleScatterGatherMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF:
		//TODO: IMPLEMENTAR BROADCAST DE EOF
	default:
		nack()
	}
}

func (pf *OriginDestinationRouter) handleScatterGatherMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	//TODO: IMPLEMENTAR LÓGICA DE RUTEO
}
