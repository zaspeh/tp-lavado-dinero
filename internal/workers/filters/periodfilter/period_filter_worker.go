package periodfilter

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeRoute struct {
	Queue  middleware.Middleware
	Period Period
}

type PeriodFilterWorker struct {
	usdInputQueue middleware.Middleware
	rawInputQueue middleware.Middleware

	avgByTypeRoutes []AvgByTypeRoute

	scatterGatherPeriod     Period
	groupByOriginQueue      middleware.Middleware
	groupByDestinationQueue middleware.Middleware

	paymentTypePeriod      Period
	paymentTypeFilterQueue middleware.Middleware
}

type PeriodFilterWorkerConfig struct {
	UsdInputQueueName string
	RawInputQueueName string

	AvgByTypeQueueNames []string
	AvgByTypePeriods    []Period

	ScatterGatherPeriod         Period
	GroupByOriginQueueName      string
	GroupByDestinationQueueName string

	PaymentTypePeriod          Period
	PaymentTypeFilterQueueName string

	MomHost string
	MomPort int
}

func NewPeriodFilterWorker(config PeriodFilterWorkerConfig) (*PeriodFilterWorker, error) {
	if len(config.AvgByTypePeriods) != len(config.AvgByTypeQueueNames) {
		return nil, errors.New("period count must match avgByType queue count")
	}

	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	var createdQueues []middleware.Middleware

	closeAllQueues := func() {
		for _, queue := range createdQueues {
			queue.Close()
		}
	}

	usdInputQueue, err := middleware.CreateQueueMiddleware(config.UsdInputQueueName, connSettings)
	if err != nil {
		return nil, err
	}
	createdQueues = append(createdQueues, usdInputQueue)

	rawInputQueue, err := middleware.CreateQueueMiddleware(config.RawInputQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, rawInputQueue)

	var avgByTypeRoutes []AvgByTypeRoute
	for i, queueName := range config.AvgByTypeQueueNames {
		queue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			closeAllQueues()
			return nil, err
		}
		avgByTypeRoutes = append(avgByTypeRoutes, AvgByTypeRoute{
			Queue:  queue,
			Period: config.AvgByTypePeriods[i],
		})
		createdQueues = append(createdQueues, queue)
	}

	groupByOriginQueue, err := middleware.CreateQueueMiddleware(config.GroupByOriginQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, groupByOriginQueue)

	groupByDestinationQueue, err := middleware.CreateQueueMiddleware(config.GroupByDestinationQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, groupByDestinationQueue)

	paymentTypeFilterQueue, err := middleware.CreateQueueMiddleware(config.PaymentTypeFilterQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, paymentTypeFilterQueue)

	return &PeriodFilterWorker{
		usdInputQueue:           usdInputQueue,
		rawInputQueue:           rawInputQueue,
		avgByTypeRoutes:         avgByTypeRoutes,
		scatterGatherPeriod:     config.ScatterGatherPeriod,
		groupByOriginQueue:      groupByOriginQueue,
		groupByDestinationQueue: groupByDestinationQueue,
		paymentTypePeriod:       config.PaymentTypePeriod,
		paymentTypeFilterQueue:  paymentTypeFilterQueue,
	}, nil
}

func (pf *PeriodFilterWorker) Run() error {
	go pf.usdInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		pf.handleUSDMessage(msg, ack, nack)
	})
	go pf.rawInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		pf.handleRawMessage(msg, ack, nack)
	})

	go pf.handleSignals()
	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (pf *PeriodFilterWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	pf.usdInputQueue.Close()
	pf.rawInputQueue.Close()
	for _, route := range pf.avgByTypeRoutes {
		route.Queue.Close()
	}
	pf.groupByOriginQueue.Close()
	pf.groupByDestinationQueue.Close()
	pf.paymentTypeFilterQueue.Close()
}

func (pf *PeriodFilterWorker) handleUSDMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_PERIODFILTER:
		pf.handlePeriodFilterMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF:
		//TODO: IMPLEMENTAR BROADCAST DE EOF
	default:
		nack()
	}
}

func (pf *PeriodFilterWorker) handleRawMessage(msg middleware.Message, ack, nack func()) {
	// Implementation for handling raw messages
}

func (pf *PeriodFilterWorker) handlePeriodFilterMessage(moneyLaundry *protobuf.MoneyLaundry, rawMsg middleware.Message, ack, nack func()) {
	periodFilterMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.PeriodFilter{})
	if err != nil {
		nack()
		return
	}

	transactionTime := periodFilterMsg.GetTimestamp().AsTime()
	for _, route := range pf.avgByTypeRoutes {
		if route.Period.Contains(transactionTime) {
			//enviar mensaje con campos para avg by type a route.Queue
		}
	}

	if pf.scatterGatherPeriod.Contains(transactionTime) {
		//enviar mensaje con campos para scatter gather a GroupBy queues
	}
}
