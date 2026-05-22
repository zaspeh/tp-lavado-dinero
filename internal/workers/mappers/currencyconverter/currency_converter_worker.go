package currencyconverter

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type CurrencyConverterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	Converter       Converter
}

type CurrencyConverterWorker struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	converter   Converter
}

func NewCurrencyConverterWorker(cfg CurrencyConverterConfig) (*CurrencyConverterWorker, error) {
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

	return &CurrencyConverterWorker{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		converter:   cfg.Converter,
	}, nil
}

func (w *CurrencyConverterWorker) Run() error {
	go w.handleSignals()

	err := w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		w.handleMessage(msg, ack, nack)
	})

	return err
}

func (w *CurrencyConverterWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	w.inputQueue.Close()
	w.outputQueue.Close()
}

func (w *CurrencyConverterWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundering, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT:
		w.handleConvertMessage(moneyLaundering, ack, nack)
	case protobuf.MessageType_EOF_:
		w.handleEOFMessage(msg, ack, nack)
	default:
		nack()
	}
}

func (w *CurrencyConverterWorker) handleConvertMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	toConvertMsg, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.ToConvertTypeFilteredPayment{})
	if err != nil {
		nack()
		return
	}

	currency := toConvertMsg.GetPaymentCurrency()
	amount, err := strconv.ParseFloat(toConvertMsg.GetAmountPaid(), 64)
	if err != nil {
		nack()
		return
	}

	convertedAmount, err := w.converter.ConvertToUSD(currency, amount)
	if err == ErrorCurrencyNotFound {
		// SI no se encuentra la moneda, por el momento se filtra
		ack()
		return
	}

	if err := w.sendConvertedMessage(convertedAmount); err != nil {
		nack()
		return
	}
	ack()
}

func (w *CurrencyConverterWorker) sendConvertedMessage(convertedAmount float64) error {
	return nil
}

func (w *CurrencyConverterWorker) handleEOFMessage(msg middleware.Message, ack, nack func()) {
	slog.Info("EOF message received")
	if err := w.outputQueue.Send(msg); err != nil {
		nack()
		return
	}
	ack()
}
