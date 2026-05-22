package formatfilter

import (
	"log/slog"
	"os"
	"os/signal"
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
		w.handleEOFMessage(moneyLaundering, ack, nack)
	default:
		nack()
	}
}

func (w *FormatFilterWorker) handlePeriodFilterdMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {

}

func (w *FormatFilterWorker) handleEOFMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
}
