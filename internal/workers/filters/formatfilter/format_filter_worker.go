package formatfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"slices"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type FormatFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AllowedFormats  []string
	WorkerID        int
	WorkerCount     int
	WorkerExchange  string
}

type FormatFilterWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	allowedFormats []string
	coordinator    *c.EOFCoordinator
	batchers       map[string]*batch.Batcher[*protobuf.ToConvertTypeFilteredPayment, *protobuf.ToConvertTypeFilteredPaymentBatch]
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

	worker := &FormatFilterWorker{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		allowedFormats: cfg.AllowedFormats,
		batchers:       make(map[string]*batch.Batcher[*protobuf.ToConvertTypeFilteredPayment, *protobuf.ToConvertTypeFilteredPaymentBatch]),
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: cfg.WorkerExchange,
		ConnSettings:      connSettings,
		WorkerID:          cfg.WorkerID,
		WorkerCount:       cfg.WorkerCount,
		FlushHandler:      worker.sendEOFMessage,
	}

	coordinator, err := c.NewEOFCoordinator(coordinatorConfig)
	if err != nil {
		inputQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	worker.coordinator = coordinator
	return worker, nil
}

func (w *FormatFilterWorker) Run() error {
	go w.handleSignals()
	go w.coordinator.Run()

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
	moneyLaundering, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED_BATCH:
		w.handlePeriodFilteredBatch(moneyLaundering, ack, nack)
	case protobuf.MessageType_EOF_:
		w.handleEOFMessage(moneyLaundering, ack, nack)
	default:
		nack()
	}
}

func (w *FormatFilterWorker) handlePeriodFilteredBatch(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundering.GetClientID()
	batcher := w.getBatcher(clientID)

	periodFilteredBatch := moneyLaundering.GetToConvertPeriodFilteredBatch()
	for _, periodFilteredMsg := range periodFilteredBatch.GetItems() {
		if !w.isAllowedFormat(periodFilteredMsg.GetPaymentFormat()) {
			if err := w.coordinator.RecordProcessed(clientID); err != nil {
				nack()
				return
			}
			continue
		}

		filteredMsg := &protobuf.ToConvertTypeFilteredPayment{
			AmountPaid:      periodFilteredMsg.GetAmountPaid(),
			PaymentCurrency: periodFilteredMsg.GetPaymentCurrency(),
		}
		if err := batcher.Add(filteredMsg); err != nil {
			nack()
			return
		}
		if err := w.coordinator.RecordSurvivor(clientID); err != nil {
			nack()
			return
		}
		if err := w.coordinator.RecordProcessed(clientID); err != nil {
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

func (w *FormatFilterWorker) isAllowedFormat(format string) bool {
	return slices.Contains(w.allowedFormats, format)
}

func (w *FormatFilterWorker) handleEOFMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundering.GetClientID()
	if batcher := w.batchers[clientID]; batcher != nil {
		if err := batcher.Flush(); err != nil {
			nack()
			return
		}
	}

	eofMessage := moneyLaundering.GetEofMessage()
	if err := w.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (w *FormatFilterWorker) getBatcher(clientID string) *batch.Batcher[*protobuf.ToConvertTypeFilteredPayment, *protobuf.ToConvertTypeFilteredPaymentBatch] {
	if batcher, ok := w.batchers[clientID]; ok {
		return batcher
	}

	filteredBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.ToConvertTypeFilteredPayment](),
		protowrappers.WrapToConvertTypeFilteredPayment,
	)

	onFlush := func(batch *protobuf.ToConvertTypeFilteredPaymentBatch) error {
		return w.sendTypeFilteredBatch(clientID, batch)
	}

	batcher := batch.NewBatcher(filteredBatch, onFlush)
	w.batchers[clientID] = batcher
	return batcher
}

func (w *FormatFilterWorker) sendTypeFilteredBatch(clientID string, batch *protobuf.ToConvertTypeFilteredPaymentBatch) error {
	innerMessage := &protobuf.MoneyLaundry_ToConvertTypeFilteredPaymentBatch{
		ToConvertTypeFilteredPaymentBatch: batch,
	}
	serializedMsg, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT_BATCH,
		innerMessage,
	)
	if err != nil {
		return err
	}

	return w.outputQueue.Send(serializedMsg)
}

func (w *FormatFilterWorker) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !w.coordinator.IsLeader() {
		return nil
	}

	slog.Info("Broadcasting EOF message", "clientID", clientID, "newEOFCount", newEOFCount)
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

	return w.outputQueue.Send(serializedEOFMessage)
}
