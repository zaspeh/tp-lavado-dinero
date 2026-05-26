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
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type AmountFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	AmountToFilter float64
	coordinator    *c.EOFCoordinator
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	AmountToFilter float64

	WorkerID           int
	WorkerCount        int
	WorkerExchangeName string
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

	amountFilter := &AmountFilter{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchangeName,
		ConnSettings:      connSettings,
		WorkerID:          config.WorkerID,
		WorkerCount:       config.WorkerCount,
		FlushHandler:      amountFilter.sendEOFMessage,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		amountFilter.outputQueue.Close()
		return nil, err
	}

	amountFilter.coordinator = coordinator

	return amountFilter, nil
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
	af.coordinator.Close()
	af.outputQueue.Close()
}

func (af *AmountFilter) Run() error {
	go af.coordinator.Run()
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

			slog.Debug("Microtransaction passed filter", "clientID", clientID, "amount", amount)

			// TODO: Si falla una transacción, se reenvia el batch y podríamos tener duplicados.
			if err := af.coordinator.RecordSurvivor(clientID); err != nil {
				nack()
				return
			}
		}

		if err := af.coordinator.RecordProcessed(clientID); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (af *AmountFilter) handleEOF(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	slog.Info("Received EOF", "clientID", moneyLaundering.GetClientID())

	eofMessage := moneyLaundering.GetEofMessage()

	if err := af.coordinator.HandleLocalEOF(moneyLaundering.GetClientID(), eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (af *AmountFilter) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !af.coordinator.IsLeader() {
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

	if err := af.outputQueue.Send(msg); err != nil {
		return err
	}

	return nil
}
