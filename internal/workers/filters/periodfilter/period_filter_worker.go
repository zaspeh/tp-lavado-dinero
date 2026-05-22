package periodfilter

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

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

	scatterGatherPeriod    Period
	originDestinationQueue middleware.Middleware

	// Q3
	query3Period1 Period
	query3Period2 Period

	paymentTypePeriod      Period
	paymentTypeFilterQueue middleware.Middleware
	paymentTypeRouterQueue middleware.Middleware
}

type PeriodFilterWorkerConfig struct {
	UsdInputQueueName string
	RawInputQueueName string

	AvgByTypeQueueNames []string
	AvgByTypePeriods    []Period

	ScatterGatherPeriod              Period
	OriginDestinationRouterQueueName string

	// Q3
	Query3Period1 Period
	Query3Period2 Period

	PaymentTypePeriod          Period
	PaymentTypeFilterQueueName string
	PaymentTypeRouterQueueName string
	MomHost                    string
	MomPort                    int
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

	originDestinationQueue, err := middleware.CreateQueueMiddleware(config.OriginDestinationRouterQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, originDestinationQueue)

	paymentTypeFilterQueue, err := middleware.CreateQueueMiddleware(config.PaymentTypeFilterQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, paymentTypeFilterQueue)

	paymentTypeRouterQueue, err := middleware.CreateQueueMiddleware(config.PaymentTypeRouterQueueName, connSettings)
	if err != nil {
		closeAllQueues()
		return nil, err
	}
	createdQueues = append(createdQueues, paymentTypeRouterQueue)

	return &PeriodFilterWorker{
		usdInputQueue:          usdInputQueue,
		rawInputQueue:          rawInputQueue,
		avgByTypeRoutes:        avgByTypeRoutes,
		scatterGatherPeriod:    config.ScatterGatherPeriod,
		originDestinationQueue: originDestinationQueue,
		query3Period1:          config.Query3Period1,
		query3Period2:          config.Query3Period2,
		paymentTypePeriod:      config.PaymentTypePeriod,
		paymentTypeFilterQueue: paymentTypeFilterQueue,
		paymentTypeRouterQueue: paymentTypeRouterQueue,
	}, nil
}

func (pf *PeriodFilterWorker) Run() error {
	go pf.handleSignals()

	go pf.rawInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		pf.handleRawMessage(msg, ack, nack)
	})

	pf.usdInputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		pf.handleUSDMessage(msg, ack, nack)
	})

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
	pf.originDestinationQueue.Close()
	pf.paymentTypeFilterQueue.Close()
	pf.paymentTypeRouterQueue.Close()
}

func (pf *PeriodFilterWorker) handleUSDMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_PERIODFILTER:
		pf.handlePeriodFilterMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		pf.handleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (pf *PeriodFilterWorker) handleRawMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_TRANSACTION:
		pf.handleTransactionMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		// funcionalidad a revisar, hay problemas de EOF
		pf.handleEOFMessageFromRaw(msg, ack, nack)
	default:
		nack()
	}
}

func (pf *PeriodFilterWorker) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, rawMsg middleware.Message, ack, nack func()) {
	// At the moment, we just acknowledge and forward the EOF message to all output queues. Depending on the requirements, we might want to implement more complex logic here.
	//handling errors

	for _, route := range pf.avgByTypeRoutes {
		if err := route.Queue.Send(rawMsg); err != nil {
			nack()
			return
		}
	}

	if err := pf.originDestinationQueue.Send(rawMsg); err != nil {
		nack()
		return
	}

	// if err := pf.paymentTypeFilterQueue.Send(rawMsg); err != nil {
	// 	nack()
	// 	return
	// }

	if err := pf.paymentTypeRouterQueue.Send(rawMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (pf *PeriodFilterWorker) handlePeriodFilterMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	periodFilterMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.PeriodFilter{})
	if err != nil {
		nack()
		return
	}

	timestamp := periodFilterMsg.GetTimestamp().AsTime()

	// filtro por periodo Q3
	err = pf.checkToPublishToPaymentTypeRouter(periodFilterMsg, moneyLaundry.GetClientID(), timestamp)
	if err != nil {
		nack()
		return
	}

	err = pf.publishScatterGatherMessage(periodFilterMsg)
	if err != nil {
		nack()
		return
	}

	ack()
}

func (pf *PeriodFilterWorker) handleTransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	transactionMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.ToConvertTransaction{})
	if err != nil {
		nack()
		return
	}

	if !pf.paymentTypePeriod.Contains(transactionMsg.GetTimestamp().AsTime()) {
		ack()
		return
	}

	if err := pf.publishToPaymentTypeQueue(transactionMsg); err != nil {
		nack()
		return
	}
	ack()
}

func (pf *PeriodFilterWorker) publishScatterGatherMessage(periodFilterMsg *protobuf.PeriodFilter) error {
	if !pf.scatterGatherPeriod.Contains(periodFilterMsg.GetTimestamp().AsTime()) {
		return nil
	}
	scatterGatherMsg := &protobuf.ScatterGather{
		FromBank:  periodFilterMsg.GetFromBank(),
		ToBank:    periodFilterMsg.GetToBank(),
		Account:   periodFilterMsg.GetAccount(),
		ToAccount: periodFilterMsg.GetToAccount(),
	}

	serializedMsg, err := serializer.SerializeProtoMessage(scatterGatherMsg, protobuf.MessageType_SCATTERGATHER)
	if err != nil {
		return err
	}

	if err := pf.originDestinationQueue.Send(*serializedMsg); err != nil {
		return err
	}

	return nil
}

func (pf *PeriodFilterWorker) checkToPublishToPaymentTypeRouter(periodFilterMsg *protobuf.PeriodFilter, clientID string, timestamp time.Time) error {
	if pf.query3Period1.Contains(timestamp) {
		err := pf.publishToPaymentTypeRouter(periodFilterMsg, clientID, protobuf.MessageType_AVGBYTYPE_FIRST_PERIOD)
		if err != nil {
			return err
		}
	}

	if pf.query3Period2.Contains(timestamp) {
		err := pf.publishToPaymentTypeRouter(periodFilterMsg, clientID, protobuf.MessageType_AVGBYTYPE_SECOND_PERIOD)
		if err != nil {
			return err
		}
	}

	return nil
}

func (pf *PeriodFilterWorker) publishToPaymentTypeRouter(periodFilterMsg *protobuf.PeriodFilter, clientID string, messageType protobuf.MessageType) error {

	avgByTypeTransaction := &protobuf.AvgByTypeTransaction{
		Account:       periodFilterMsg.GetAccount(),
		AmountPaid:    periodFilterMsg.GetAmountPaid(),
		PaymentFormat: periodFilterMsg.GetPaymentFormat(),
	}

	serializedMsg, err := serializer.SerializeProtoMessageWithClientID(clientID, avgByTypeTransaction, messageType)

	if err != nil {
		return err
	}

	return pf.paymentTypeRouterQueue.Send(*serializedMsg)
}

func (pf *PeriodFilterWorker) publishToPaymentTypeQueue(transactionMsg *protobuf.ToConvertTransaction) error {
	filteredPeriodMsg := &protobuf.ToConvertPeriodFiltered{
		AmountPaid:      transactionMsg.GetAmountPaid(),
		PaymentCurrency: transactionMsg.GetPaymentCurrency(),
		PaymentFormat:   transactionMsg.GetPaymentFormat(),
	}
	serializedMsg, err := serializer.SerializeProtoMessage(filteredPeriodMsg, protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED)
	if err != nil {
		return err
	}

	return pf.paymentTypeFilterQueue.Send(*serializedMsg)
}

func (pf *PeriodFilterWorker) handleEOFMessageFromRaw(msg middleware.Message, ack, nack func()) {
	if err := pf.paymentTypeFilterQueue.Send(msg); err != nil {
		nack()
		return
	}
	ack()
}
