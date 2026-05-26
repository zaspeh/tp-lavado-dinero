package filters

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

type AmountFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	AmountToFilter float64
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	AmountToFilter float64
}

func NewAmountFilter(config AmountFilterConfig) (*AmountFilter, error) {
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

	return &AmountFilter{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,
	}, nil
}

func (af *AmountFilter) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	af.inputQueue.Close()
	af.outputQueue.Close()
}

func (af *AmountFilter) Run() error {
	go af.handleSignals()
	return af.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		af.handleMessage(msg, ack, nack)
	})
}

func (af *AmountFilter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundering, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_MICROTRANSACTION_BATCH:
		af.handleMicrotransactionMessage(moneyLaundering, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		af.handleEOF(moneyLaundering, msg, ack, nack)
	default:
		nack()
	}
}

func (af *AmountFilter) handleMicrotransactionMessage(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	microtransactionBatch := moneyLaundering.GetMicrotransactionsBatch()
	clientID := moneyLaundering.GetClientID()
	for _, microtransaction := range microtransactionBatch.GetItems() {
		amount, err := strconv.ParseFloat(microtransaction.GetAmountPaid(), 64)
		if err != nil {
			nack()
			return
		}

		if amount < af.AmountToFilter {
			// TODO: cambiar a batch de ser necesario
			serializedMsg, err := serializer.SerializeProtoMessageWithClientID(clientID, microtransaction, protobuf.MessageType_MICROTRANSACTION)
			if err != nil {
				nack()
				return
			}

			if err := af.outputQueue.Send(*serializedMsg); err != nil {
				nack()
				return
			}
		}
	}
	ack()
}

func (af *AmountFilter) handleEOF(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	slog.Info("Received EOF", "clientID", moneyLaundering.GetClientID())
	if err := af.outputQueue.Send(msg); err != nil {
		nack()
		return
	}
	ack()
}
