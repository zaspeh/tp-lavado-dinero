package periodfilter

import (
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
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

	query4Coordinator *c.EOFCoordinator

	rawCoordinator *c.EOFCoordinator
	rawBatchers    map[string]*batch.Batcher[*protobuf.ToConvertPeriodFiltered, *protobuf.ToConvertPeriodFilteredBatch]
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

	RawWorkerID           int
	RawWorkerCount        int
	RawWorkerExchangeName string

	//Q4
	Query4WorkerID           int
	Query4WorkerCount        int
	Query4WorkerExchangeName string
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

	periodFilterWorker := &PeriodFilterWorker{
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
		rawBatchers:            make(map[string]*batch.Batcher[*protobuf.ToConvertPeriodFiltered, *protobuf.ToConvertPeriodFilteredBatch]),
	}

	rawCoordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.RawWorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.RawWorkerID,
		WorkerCount:       config.RawWorkerCount,
		FlushHandler:      periodFilterWorker.sendRawEOFMessage,
	}

	rawCoordinator, err := c.NewEOFCoordinator(rawCoordinatorConfig)
	if err != nil {
		closeAllQueues()
		return nil, err
	}

	periodFilterWorker.rawCoordinator = rawCoordinator

	query4CoordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.Query4WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.Query4WorkerID,
		WorkerCount:       config.Query4WorkerCount,
		FlushHandler:      periodFilterWorker.sendQuery4EOFMessage,
	}

	query4Coordinator, err := c.NewEOFCoordinator(
		query4CoordinatorConfig,
	)

	if err != nil {
		periodFilterWorker.rawCoordinator.Close()
		closeAllQueues()
		return nil, err
	}

	periodFilterWorker.query4Coordinator = query4Coordinator

	return periodFilterWorker, nil
}

func (pf *PeriodFilterWorker) Run() error {
	go pf.handleSignals()
	go pf.rawCoordinator.Run()
	go pf.query4Coordinator.Run()

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
	pf.rawCoordinator.Close()
	pf.query4Coordinator.Close()
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
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_TO_CONVERT_TRANSACTION_BATCH:
		pf.handleToConvertBatch(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		pf.handleEOFMessageFromRaw(moneyLaundry, ack, nack)
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

	clientID := moneyLaundry.GetClientID()
	eofMessage := moneyLaundry.GetEofMessage()
	if err := pf.query4Coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}

	if err := pf.paymentTypeRouterQueue.Send(rawMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (pf *PeriodFilterWorker) handlePeriodFilterMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	periodFilterMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.PeriodFilter{})
	clientID := moneyLaundry.GetClientID()
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

	err = pf.publishScatterGatherMessage(periodFilterMsg, clientID)
	if err != nil {
		nack()
		return
	}

	ack()
}

func (pf *PeriodFilterWorker) handleToConvertBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	toConvertBatch := moneyLaundry.GetToConvertBatch()
	clientID := moneyLaundry.GetClientID()
	items := toConvertBatch.GetItems()
	batcher := pf.getRawBatcher(clientID)
	for _, transactionMsg := range items {
		if !pf.paymentTypePeriod.Contains(transactionMsg.GetTimestamp().AsTime()) {
			if err := pf.rawCoordinator.RecordProcessed(clientID); err != nil {
				nack()
				return
			}
			continue
		}

		filteredPeriodMsg := &protobuf.ToConvertPeriodFiltered{
			AmountPaid:      transactionMsg.GetAmountPaid(),
			PaymentCurrency: transactionMsg.GetPaymentCurrency(),
			PaymentFormat:   transactionMsg.GetPaymentFormat(),
		}
		if err := batcher.Add(filteredPeriodMsg); err != nil {
			nack()
			return
		}
		if err := pf.rawCoordinator.RecordSurvivor(clientID); err != nil {
			nack()
			return
		}
		if err := pf.rawCoordinator.RecordProcessed(clientID); err != nil {
			nack()
			return
		}
	}
	if err := batcher.Flush(); err != nil {
		nack()
		return
	}
	ack()
}

func (pf *PeriodFilterWorker) publishScatterGatherMessage(periodFilterMsg *protobuf.PeriodFilter, clientID string) error {
	if !pf.scatterGatherPeriod.Contains(periodFilterMsg.GetTimestamp().AsTime()) {
		return pf.query4Coordinator.RecordProcessed(clientID)
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

	if err := pf.query4Coordinator.RecordProcessed(clientID); err != nil {
		return err
	}

	if err := pf.query4Coordinator.RecordSurvivor(clientID); err != nil {
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

func (pf *PeriodFilterWorker) handleEOFMessageFromRaw(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	if batcher := pf.rawBatchers[clientID]; batcher != nil {
		if err := batcher.Flush(); err != nil {
			nack()
			return
		}
	}

	eofMessage := moneyLaundry.GetEofMessage()
	if err := pf.rawCoordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (pf *PeriodFilterWorker) getRawBatcher(clientID string) *batch.Batcher[*protobuf.ToConvertPeriodFiltered, *protobuf.ToConvertPeriodFilteredBatch] {
	if batcher, ok := pf.rawBatchers[clientID]; ok {
		return batcher
	}

	convertedBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.ToConvertPeriodFiltered](),
		protowrappers.WrapToConvertPeriodFiltered,
	)

	onFlush := func(batch *protobuf.ToConvertPeriodFilteredBatch) error {
		return pf.publishToPaymentTypeQueue(clientID, batch)
	}

	batcher := batch.NewBatcher(convertedBatch, onFlush)
	pf.rawBatchers[clientID] = batcher
	return batcher
}

func (pf *PeriodFilterWorker) publishToPaymentTypeQueue(clientID string, batch *protobuf.ToConvertPeriodFilteredBatch) error {
	innerMessage := &protobuf.MoneyLaundry_ToConvertPeriodFilteredBatch{
		ToConvertPeriodFilteredBatch: batch,
	}
	serializedMsg, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED_BATCH,
		innerMessage,
	)
	if err != nil {
		return err
	}

	return pf.paymentTypeFilterQueue.Send(serializedMsg)
}

func (pf *PeriodFilterWorker) sendRawEOFMessage(clientID string, newEOFCount uint64) error {
	if !pf.rawCoordinator.IsLeader() {
		return nil
	}

	slog.Info("Broadcasting raw EOF message", "clientID", clientID, "newEOFCount", newEOFCount)
	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: newEOFCount,
		},
	}

	serializedEOFMessage, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_EOF_,
		eofMessage,
	)
	if err != nil {
		return err
	}

	return pf.paymentTypeFilterQueue.Send(serializedEOFMessage)
}

func (pf *PeriodFilterWorker) sendQuery4EOFMessage(clientID string, newEOFCount uint64) error {
	if !pf.query4Coordinator.IsLeader() {
		return nil
	}

	slog.Info("Broadcasting query4 EOF message", "clientID", clientID, "newEOFCount", newEOFCount)
	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: newEOFCount,
		},
	}

	serializedEOFMessage, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_EOF_,
		eofMessage,
	)
	if err != nil {
		return err
	}

	return pf.originDestinationQueue.Send(serializedEOFMessage)
}
