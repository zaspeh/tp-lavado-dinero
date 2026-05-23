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
)

const eofRoutingKey = "eof"

type PaymentTypeRouter struct {
	inputQueue middleware.Middleware

	paymentTypeExchange *middleware.ExchangeMiddleware

	avgByTypeExchangeKeys []string
	maxWorkersAmount      int
}

type PaymentTypeRouterConfig struct {
	InputQueueName string

	PaymentTypeExchangeName string

	AvgByTypeWorkerAmount int

	MomHost string
	MomPort int
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

	return &PaymentTypeRouter{
		inputQueue:            inputQueue,
		paymentTypeExchange:   paymentTypeExchange,
		avgByTypeExchangeKeys: avgByTypeExchangeKeys,
		maxWorkersAmount:      config.AvgByTypeWorkerAmount,
	}, nil
}

func (r *PaymentTypeRouter) Run() error {
	go r.handleSignals()

	r.inputQueue.StartConsuming(
		func(msg middleware.Message, ack, nack func()) {
			r.handleMessage(msg, ack, nack)
		},
	)

	return nil
}

func (r *PaymentTypeRouter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {

	case protobuf.MessageType_AVGBYTYPE_FIRST_PERIOD,
		protobuf.MessageType_AVGBYTYPE_SECOND_PERIOD:
		r.handleAvgByTypeTransaction(msg, moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		r.handleEOFMessage(msg, ack, nack)

	default:
		nack()
	}
}

func (r *PaymentTypeRouter) handleAvgByTypeTransaction(msg middleware.Message, moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	transaction, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.AvgByTypeTransaction{})
	if err != nil {
		nack()
		return
	}
	slog.Info(
		"routing avg by type transaction",
		"type", moneyLaundry.GetType(),
		"paymentFormat", transaction.GetPaymentFormat(),
	)

	workerKey := r.selectWorkerKey(transaction.GetPaymentFormat())

	slog.Debug(
		"routing payment format",
		"format", workerKey,
		"clientID", moneyLaundry.GetClientID(),
	)

	if err := r.paymentTypeExchange.SendWithKey(workerKey, msg); err != nil {
		nack()
		return
	}

	ack()
}

func (r *PaymentTypeRouter) handleEOFMessage(msg middleware.Message, ack, nack func()) {

	slog.Info("routing EOF to payment type workers")

	for _, key := range r.avgByTypeExchangeKeys {
		if err := r.paymentTypeExchange.SendWithKey(key, msg); err != nil {
			nack()
			return
		}
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
	r.paymentTypeExchange.Close()
}

func (r *PaymentTypeRouter) selectWorkerKey(paymentFormat string) string {

	h := fnv.New32a()

	_, _ = h.Write([]byte(paymentFormat))

	return r.avgByTypeExchangeKeys[h.Sum32()%uint32(r.maxWorkersAmount)]
}
