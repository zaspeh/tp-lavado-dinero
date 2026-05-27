package routers

import (
	"fmt"
	"hash/fnv"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

const eofRoutingKey = "eof"

type PaymentTypeRouter struct {
	inputQueue middleware.Middleware

	paymentTypeExchange *middleware.ExchangeMiddleware

	avgByTypeExchangeKeys []string
	maxWorkersAmount      int

	coordinator *c.EOFCoordinator
}

type PaymentTypeRouterConfig struct {
	InputQueueName string

	PaymentTypeExchangeName string

	AvgByTypeWorkerAmount int

	MomHost string
	MomPort int

	WorkerID           int
	WorkerCount        int
	WorkerExchangeName string
}

func NewPaymentTypeRouter(config PaymentTypeRouterConfig) (*PaymentTypeRouter, error) {

	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	avgByTypeExchangeKeys := make([]string, config.AvgByTypeWorkerAmount)

	for i := range avgByTypeExchangeKeys {
		avgByTypeExchangeKeys[i] = fmt.Sprintf("%s.%d", config.PaymentTypeExchangeName, i)
	}

	paymentTypeExchange, err := middleware.CreateExchangeMiddleware(config.PaymentTypeExchangeName, avgByTypeExchangeKeys, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	paymentTypeRouter := &PaymentTypeRouter{
		inputQueue:            inputQueue,
		paymentTypeExchange:   paymentTypeExchange,
		avgByTypeExchangeKeys: avgByTypeExchangeKeys,
		maxWorkersAmount:      config.AvgByTypeWorkerAmount,
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.WorkerID,
		WorkerCount:       config.WorkerCount,
		FlushHandler:      paymentTypeRouter.sendEOFMessage,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		paymentTypeExchange.Close()
		return nil, err
	}

	paymentTypeRouter.coordinator = coordinator

	return paymentTypeRouter, nil
}

func (r *PaymentTypeRouter) Run() error {
	go r.coordinator.Run()
	go r.handleSignals()

	slog.Info("PAYMENT TYPE ROUTER STARTED")

	r.inputQueue.StartConsuming(
		func(msg middleware.Message, ack, nack func()) {
			r.handleMessage(msg, ack, nack)
		},
	)

	return nil
}

func (r *PaymentTypeRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH:
		r.handleAvgByTypeBatch(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		r.handleEOFMessage(msg, ack, nack)
	default:
		nack()
	}
}

func (r *PaymentTypeRouter) handleAvgByTypeBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	avgTypeBatch := moneyLaundry.GetAvgbytypeTransactionBatch()
	items := avgTypeBatch.GetItems()
	clientID := moneyLaundry.GetClientID()
	batchesByKey := make(map[string][]*protobuf.AvgByTypeTransaction)
	for _, tx := range items {
		workerKey := r.selectWorkerKey(tx.GetPaymentFormat())
		batchesByKey[workerKey] = append(batchesByKey[workerKey], tx)
	}

	for workerKey, batchMessages := range batchesByKey {
		inner := &protobuf.MoneyLaundry_AvgbytypeTransactionBatch{
			AvgbytypeTransactionBatch: &protobuf.AvgByTypeTransactionBatch{
				Items: batchMessages},
		}

		msg, err := protobuf.SerializeProtoMessageONTRIAL(
			clientID,
			protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH,
			inner,
		)

		if err != nil {
			nack()
			return
		}

		if err := r.paymentTypeExchange.SendWithKey(workerKey, msg); err != nil {
			nack()
			return
		}
	}

	for range items {
		if err := r.coordinator.RecordProcessed(clientID); err != nil {
			nack()
			return
		}
		if err := r.coordinator.RecordSurvivor(clientID); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (r *PaymentTypeRouter) handleEOFMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	eofMessage := moneyLaundry.GetEofMessage()
	if err := r.coordinator.HandleLocalEOF(moneyLaundry.GetClientID(), eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (r *PaymentTypeRouter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	r.inputQueue.Close()
	r.coordinator.Close()
	r.paymentTypeExchange.Close()
}

func (r *PaymentTypeRouter) selectWorkerKey(paymentFormat string) string {
	h := fnv.New32a()
	_, _ = h.Write([]byte(paymentFormat))
	return r.avgByTypeExchangeKeys[h.Sum32()%uint32(r.maxWorkersAmount)]
}

func (r *PaymentTypeRouter) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !r.coordinator.IsLeader() {
		return nil
	}

	slog.Info("coordinator triggered flush handler, sending EOF message", "clientID", clientID)

	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: newEOFCount,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMessage)
	if err != nil {
		return err
	}

	if err := r.paymentTypeExchange.Send(msg); err != nil {
		return err
	}

	return nil
}
