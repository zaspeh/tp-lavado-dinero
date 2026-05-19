package periodfilter

import (
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
