package currencyconverter

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
)

type CurrencyConverterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	Converter       Converter
	WorkerID        int
	WorkerCount     int
	WorkerExchange  string
}

type CurrencyConverterWorker struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	converter   Converter
	coordinator *c.EOFCoordinator
	batchers    map[string]*batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]
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

	worker := &CurrencyConverterWorker{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		converter:   cfg.Converter,
		batchers:    make(map[string]*batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]),
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

func (w *CurrencyConverterWorker) Run() error {
	go w.handleSignals()
	go w.coordinator.Run()

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
	moneyLaundering, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundering.GetType() {
	case protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT_BATCH:
		w.handleConvertBatch(moneyLaundering, ack, nack)
	case protobuf.MessageType_EOF_:
		w.handleEOFMessage(moneyLaundering, ack, nack)
	default:
		nack()
	}
}

func (w *CurrencyConverterWorker) handleConvertBatch(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundering.GetClientID()
	batcher := w.getBatcher(clientID)
	convertBatch := moneyLaundering.GetToConvertTypeFilteredPaymentBatch()
	for _, toConvertMsg := range convertBatch.GetItems() {
		currency := toConvertMsg.GetPaymentCurrency()
		amount, err := strconv.ParseFloat(toConvertMsg.GetAmountPaid(), 64)
		if err != nil {
			nack()
			return
		}

		timestamp := toConvertMsg.GetTimestamp()
		convertedAmount, err := w.converter.ConvertToUSD(currency, amount, timestamp.AsTime())
		if err == ErrorCurrencyNotFound {
			if err := w.coordinator.RecordProcessed(clientID); err != nil {
				nack()
				return
			}
			continue
		}
		if err != nil {
			nack()
			return
		}

		convertedMsg := &protobuf.ConvertedAmount{Amount: convertedAmount}
		if err := batcher.Add(convertedMsg); err != nil {
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

func (w *CurrencyConverterWorker) handleEOFMessage(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
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

func (w *CurrencyConverterWorker) getBatcher(clientID string) *batch.Batcher[*protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch] {
	if batcher, ok := w.batchers[clientID]; ok {
		return batcher
	}

	convertedBatch := batch.New(
		0,
		protowrappers.ProtoSizer[*protobuf.ConvertedAmount](),
		protowrappers.WrapConvertedAmounts,
	)

	onFlush := func(batch *protobuf.ConvertedAmountBatch) error {
		return w.sendConvertedBatch(clientID, batch)
	}

	batcher := batch.NewBatcher(convertedBatch, onFlush)
	w.batchers[clientID] = batcher
	return batcher
}

func (w *CurrencyConverterWorker) sendConvertedBatch(clientID string, batch *protobuf.ConvertedAmountBatch) error {
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

	return w.outputQueue.Send(serializedMsg)
}

func (w *CurrencyConverterWorker) sendEOFMessage(clientID string, newEOFCount uint64) error {
	if !w.coordinator.IsLeader() {
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

	return w.outputQueue.Send(serializedEOFMessage)
}
