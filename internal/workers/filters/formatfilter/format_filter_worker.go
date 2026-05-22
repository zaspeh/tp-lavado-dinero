package formatfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type FormatFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AllowedFormats  []string
}

type FormatFilterWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	allowedFormats []string
}

func NewFormatFilterWorker(cfg FormatFilterConfig) (*FormatFilterWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: cfg.MomHost,
		Port:     cfg.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(cfg.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(cfg.OutputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &FormatFilterWorker{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		allowedFormats: cfg.AllowedFormats,
	}, nil
}

func (w *FormatFilterWorker) Run() error {
	go w.handleSignals()

	w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		w.handleMessage(msg, ack, nack)
	})
	return nil
}

func (w *FormatFilterWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	w.inputQueue.Close()
	w.outputQueue.Close()
}

func (w *FormatFilterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundering, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED:
		w.handlePeriodFilterdMessage(moneyLaundering, ack, nack)
	case protobuf.MessageType_EOF_:
		w.handleEOFMessage(msg, ack, nack)
	default:
		nack()
	}
}

func (w *FormatFilterWorker) handlePeriodFilterdMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	periodFilteredMsg, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.ToConvertPeriodFiltered{})
	if err != nil {
		nack()
		return
	}

	if !w.isAllowedFormat(periodFilteredMsg.GetPaymentFormat()) {
		nack()
		return
	}

	if err := w.sendFormatFilteredMessage(periodFilteredMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (w *FormatFilterWorker) isAllowedFormat(format string) bool {
	return slices.Contains(w.allowedFormats, format)
}

func (w *FormatFilterWorker) sendFormatFilteredMessage(periodFiltered *protobuf.ToConvertPeriodFiltered) error {
	formatFilteredMsg := &protobuf.ToConvertTypeFilteredPayment{
		AmountPaid:      periodFiltered.GetAmountPaid(),
		PaymentCurrency: periodFiltered.GetPaymentCurrency(),
	}
	serializedMsg, err := serializer.SerializeProtoMessageWithClientID("x", formatFilteredMsg, protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT)
	if err != nil {
		return err
	}

	return w.outputQueue.Send(*serializedMsg)
}

func (w *FormatFilterWorker) handleEOFMessage(msg middleware.Message, ack, nack func()) {
	slog.Info("EOF message received")
	if err := w.outputQueue.Send(msg); err != nil {
		nack()
		return
	}
	ack()
}
