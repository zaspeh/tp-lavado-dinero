package periodfilter

import (
	"errors"

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
