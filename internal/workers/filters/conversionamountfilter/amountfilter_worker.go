package conversionamountfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AmountFilterWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	AmountToFilter float64
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AmountToFilter  float64
}

func NewAmountFilter(config AmountFilterConfig) (*AmountFilterWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &AmountFilterWorker{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,
	}, nil
}

func (af *AmountFilterWorker) Run() error {
	go af.handleSignals()
	err := af.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		af.handleMessage(msg, ack, nack)

	})

	return err
}

func (w *AmountFilterWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	w.inputQueue.Close()
	w.outputQueue.Close()
}

func (af *AmountFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundering, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_CONVERTED_AMOUNT:
		af.handleConvertedAmountMessage(moneyLaundering, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		af.handleEOFMessage(msg, ack, nack)
	default:
		nack()
	}
}

func (af *AmountFilterWorker) handleEOFMessage(msg middleware.Message, ack, nack func()) {
	slog.Info("EOF message received")
	if err := af.outputQueue.Send(msg); err != nil {
		nack()
		return
	}
	ack()
}

func (af *AmountFilterWorker) handleConvertedAmountMessage(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	convertedAmountMsg, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.ConvertedAmount{})
	if err != nil {
		nack()
		return
	}

	amount := convertedAmountMsg.GetAmount()
	if amount >= af.AmountToFilter {
		nack()
		return
	}

	if err := af.outputQueue.Send(msg); err != nil {
		nack()
		return
	}

	ack()
}
