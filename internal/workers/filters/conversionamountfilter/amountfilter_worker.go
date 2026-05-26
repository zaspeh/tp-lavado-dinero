package conversionamountfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type AmountFilterWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	AmountToFilter float64
	coordinator    *c.EOFCoordinator
	batchers       map[string]*batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AmountToFilter  float64
	WorkerID        int
	WorkerCount     int
	WorkerExchange  string
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

	worker := &AmountFilterWorker{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,
		batchers:       make(map[string]*batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]),
	}

	coordinatorConfig := c.EOFCoordinatorConfig{
		PeersExchangeName: config.WorkerExchange,
		ConnSettings:      connSettings,
		WorkerID:          config.WorkerID,
		WorkerCount:       config.WorkerCount,
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

func (af *AmountFilterWorker) Run() error {
	go af.handleSignals()
	go af.coordinator.Run()
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
	moneyLaundering, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_CONVERTED_AMOUNT_BATCH:
		af.handleConvertedAmountBatch(moneyLaundering, ack, nack)
	case protobuf.MessageType_EOF_:
		af.handleEOFMessage(moneyLaundering, ack, nack)
	default:
		nack()
	}
}

func (af *AmountFilterWorker) handleEOFMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundering.GetClientID()
	if batcher := af.batchers[clientID]; batcher != nil {
		if err := batcher.Flush(); err != nil {
			nack()
			return
		}
	}

	eofMessage := moneyLaundering.GetEofMessage()
	if err := af.coordinator.HandleLocalEOF(clientID, eofMessage.GetTotalTransactions()); err != nil {
		nack()
		return
	}
	ack()
}

func (af *AmountFilterWorker) handleConvertedAmountBatch(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundering.GetClientID()
	batcher := af.getBatcher(clientID)

	convertedBatch := moneyLaundering.GetConvertedAmountBatch()
	for _, convertedAmountMsg := range convertedBatch.GetItems() {
		amount := convertedAmountMsg.GetAmount()
		if amount >= af.AmountToFilter {
			if err := af.coordinator.RecordProcessed(clientID); err != nil {
				nack()
				return
			}
			continue
		}

		if err := batcher.Add(convertedAmountMsg); err != nil {
			nack()
			return
		}
		if err := af.coordinator.RecordSurvivor(clientID); err != nil {
			nack()
			return
		}
		if err := af.coordinator.RecordProcessed(clientID); err != nil {
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

func (af *AmountFilterWorker) getBatcher(clientID string) *batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch] {
	if batcher, ok := af.batchers[clientID]; ok {
		return batcher
	}

	convertedBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.ConvertedAmount](),
		protowrappers.WrapConvertedAmounts,
	)

	onFlush := func(batch *protobuf.ConvertedAmountBatch) error {
		return af.sendConvertedBatch(clientID, batch)
	}

	batcher := batch.NewBatcher(convertedBatch, onFlush)
	af.batchers[clientID] = batcher
	return batcher
}

func (af *AmountFilterWorker) sendConvertedBatch(clientID string, batch *protobuf.ConvertedAmountBatch) error {
	if len(batch.GetItems()) == 0 {
		return nil
	}

	innerMessage := &protobuf.MoneyLaundry_ConvertedAmountBatch{
		ConvertedAmountBatch: batch,
	}
	serializedMsg, err := protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		innerMessage,
	)
	if err != nil {
		return err
	}

	return af.outputQueue.Send(serializedMsg)
}

func (af *AmountFilterWorker) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !af.coordinator.IsLeader() {
		return nil
	}

	slog.Info("Sending EOF message", "clientID", clientID, "newEOFCount", newEOFCount)
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

	return af.outputQueue.Send(serializedEOFMessage)
}
