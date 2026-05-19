package periodfilter

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type PeriodFilterWorker struct {
	usdInputQueue           middleware.Middleware
	rawInputQueue           middleware.Middleware
	avgByTypeQueues         []middleware.Middleware
	groupByOriginQueue      middleware.Middleware
	groupByDestinationQueue middleware.Middleware
	paymentTypeFilterQueue  middleware.Middleware
	periods                 []Period
}

type PeriodFilterWorkerConfig struct {
	UsdInputQueueName           string
	RawInputQueueName           string
	AvgByTypeQueueNames         []string
	GroupByOriginQueueName      string
	GroupByDestinationQueueName string
	PaymentTypeFilterQueueName  string
	MomHost                     string
	MomPort                     int
	Periods                     []Period
}

func NewPeriodFilterWorker(config PeriodFilterWorkerConfig) (*PeriodFilterWorker, error) {
	if len(config.Periods) != len(config.AvgByTypeQueueNames) {
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

	var avgByTypeQueues []middleware.Middleware
	for _, queueName := range config.AvgByTypeQueueNames {
		queue, err := middleware.CreateQueueMiddleware(queueName, connSettings)
		if err != nil {
			closeAllQueues()
			return nil, err
		}
		avgByTypeQueues = append(avgByTypeQueues, queue)
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
		avgByTypeQueues:         avgByTypeQueues,
		groupByOriginQueue:      groupByOriginQueue,
		groupByDestinationQueue: groupByDestinationQueue,
		paymentTypeFilterQueue:  paymentTypeFilterQueue,
		periods:                 config.Periods,
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
	for _, queue := range pf.avgByTypeQueues {
		queue.Close()
	}
	pf.groupByOriginQueue.Close()
	pf.groupByDestinationQueue.Close()
	pf.paymentTypeFilterQueue.Close()
}

func (pf *PeriodFilterWorker) handleUSDMessage(msg middleware.Message, ack, nack func()) {
	// Implementation for handling USD messages
}

func (pf *PeriodFilterWorker) handleRawMessage(msg middleware.Message, ack, nack func()) {
	// Implementation for handling raw messages
}
