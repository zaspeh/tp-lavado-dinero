package maxbank

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
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type MaxBankWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	maxBankStores  map[string]*MaxBankStore
	maxBatchWeight int
}

type MaxBankWorkerConfig struct {
	ID                string
	MomHost           string
	MomPort           int
	InputExchangeName string
	OutputQueueName   string
	MaxBatchWeight    int
}

func NewMaxBankWorker(cfg MaxBankWorkerConfig) (*MaxBankWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: cfg.MomHost,
		Port:     cfg.MomPort,
	}

	inputKeys := []string{cfg.InputExchangeName + "." + cfg.ID}
	inputExchange, err := middleware.CreateExchangeMiddleware(cfg.InputExchangeName, inputKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(cfg.OutputQueueName, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &MaxBankWorker{
		inputQueue:     inputExchange,
		outputQueue:    outputQueue,
		maxBankStores:  make(map[string]*MaxBankStore),
		maxBatchWeight: cfg.MaxBatchWeight,
	}, nil
}

func (w *MaxBankWorker) Run() error {
	go w.handleSignals()

	w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		w.handleMessage(msg, ack, nack)
	})

	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (w *MaxBankWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	w.inputQueue.Close()
	w.outputQueue.Close()
}

func (w *MaxBankWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MAXBANK_BATCH:
		w.handleMaxBankBatch(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		w.handleEOF(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (w *MaxBankWorker) handleMaxBankBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	maxBankBatch := moneyLaundry.GetMaxBankBatch()

	store := w.getStore(clientID)
	for _, maxBankMsg := range maxBankBatch.GetMaxBankMessage() {
		bankID := maxBankMsg.GetFromBank()

		if meta := maxBankMsg.GetBankMetadata(); meta != nil {
			store.UpdateBankName(bankID, meta.GetBankName())
			continue
		}

		if ts := maxBankMsg.GetTransferSummary(); ts != nil {
			amountStr := ts.GetAmount()
			amountVal, err := strconv.ParseFloat(amountStr, 64)
			if err != nil {
				nack()
				return
			}

			store.UpdateMaxTransaction(bankID, ts.GetAccount(), amountVal, amountStr)
			continue
		}

		nack()
		return
	}

	ack()
}

func (w *MaxBankWorker) handleEOF(moneyLaundry *protobuf.MoneyLaundry, originalMsg middleware.Message, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	slog.Info("Received EOF message in MaxBankWorker, processing stored data")
	store := w.getStore(clientID)
	reader := store.Reader()
	batcher := w.buildResultBatcher()

	for reader.HasNext() {
		processedRecord := reader.Get()
		maxBankResult := &protobuf.MaxBankResult{
			BankName: processedRecord.BankName,
			Account:  processedRecord.Account,
			Amount:   processedRecord.AmountString,
		}

		if err := batcher.Add(maxBankResult); err != nil {
			nack()
			return
		}
		reader.Next()
	}

	if err := batcher.Flush(); err != nil {
		nack()
		return
	}

	store.Clear()
	delete(w.maxBankStores, clientID)

	if err := w.outputQueue.Send(originalMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (w *MaxBankWorker) getStore(clientID string) *MaxBankStore {
	if store, ok := w.maxBankStores[clientID]; ok {
		return store
	}

	store := NewBankStore()
	w.maxBankStores[clientID] = store
	return store
}

func (w *MaxBankWorker) buildResultBatcher() *batch.Batcher[*protobuf.MaxBankResult, *protobuf.MaxBankResultBatch] {
	sizer := protowrappers.ProtoSizer[*protobuf.MaxBankResult]()
	wrapper := protowrappers.WrapMaxBankResults
	resultBatch := batch.New(w.maxBatchWeight, sizer, wrapper)
	return batch.NewBatcher(resultBatch, w.flushResultBatch)
}

func (w *MaxBankWorker) flushResultBatch(result *protobuf.MaxBankResultBatch) error {
	msg, err := serializer.SerializeProtoMessage(result, protobuf.MessageType_MAXBANK_RESULT)
	if err != nil {
		return err
	}
	return w.outputQueue.Send(*msg)
}
