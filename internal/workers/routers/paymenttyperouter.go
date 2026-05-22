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

const eofRoutingKey = "eof"

type PaymentTypeRouter struct {
	inputQueue middleware.Middleware

	paymentTypeExchange *middleware.ExchangeMiddleware

	exchangeName string
}

type PaymentTypeRouterConfig struct {
	InputQueueName string

	PaymentTypeExchangeName string

	MomHost string
	MomPort int
}

func NewPaymentTypeRouter(
	config PaymentTypeRouterConfig,
) (*PaymentTypeRouter, error) {

	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueueName,
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	paymentTypeExchange, err := middleware.CreateExchangeMiddleware(
		config.PaymentTypeExchangeName,
		[]string{},
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &PaymentTypeRouter{
		inputQueue:          inputQueue,
		paymentTypeExchange: paymentTypeExchange,
		exchangeName:        config.PaymentTypeExchangeName,
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

func (r *PaymentTypeRouter) handleMessage(
	msg middleware.Message,
	ack,
	nack func(),
) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {

	case protobuf.MessageType_PERIODFILTER:
		r.handlePeriodFilterMessage(msg, moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		r.handleEOFMessage(msg, ack, nack)

	default:
		nack()
	}
}

func (r *PaymentTypeRouter) handlePeriodFilterMessage(
	msg middleware.Message,
	moneyLaundry *protobuf.MoneyLaundry,
	ack,
	nack func(),
) {

	transaction, err := serializer.DeserializeTransaction(
		moneyLaundry.GetPayload(),
		&protobuf.PeriodFilter{},
	)
	if err != nil {
		nack()
		return
	}

	paymentFormat := transaction.GetPaymentFormat()

	slog.Info(
		"routing payment format",
		"format", paymentFormat,
		"clientID", moneyLaundry.GetClientID(),
	)

	if err := r.paymentTypeExchange.SendWithKey(paymentFormat, msg); err != nil {
		nack()
		return
	}

	ack()
}

func (r *PaymentTypeRouter) handleEOFMessage(msg middleware.Message, ack, nack func()) {

	slog.Info("routing EOF to payment type workers")

	if err := r.paymentTypeExchange.SendWithKey(eofRoutingKey, msg); err != nil {
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
	r.paymentTypeExchange.Close()
}
