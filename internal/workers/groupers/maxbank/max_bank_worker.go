package maxbank

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

type MaxBankWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	maxBankStorage *MaxBankStore
	maxBatchWeight int
}

type MaxBankWorkerConfig struct {
	MomHost           string
	MomPort           int
	InputExchangeName string
	InputKeys         []string
	OutputQueueName   string
	MaxBatchWeight    int
}

func NewMaxBankWorker(cfg MaxBankWorkerConfig) (*MaxBankWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: cfg.MomHost,
		Port:     cfg.MomPort,
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(cfg.InputExchangeName, cfg.InputKeys, connSettings)
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
		maxBankStorage: NewBankStore(),
	}, nil
}

func (w *MaxBankWorker) Run() error {
	go w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		w.handleMessage(msg, ack, nack)
	})

	go w.handleSignals()
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
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MAXBANK:
		w.handleMaxBankMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF:
		w.handleEOF(msg, ack, nack)
	default:
		nack()
	}
}

func (w *MaxBankWorker) handleMaxBankMessage(moneyLaundry *protobuf.MoneyLaundry, rawMsg middleware.Message, ack, nack func()) {
	maxBankMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.MaxBank{})
	if err != nil {
		nack()
		return
	}

	bankID := maxBankMsg.GetFromBank()

	if meta := maxBankMsg.GetBankMetadata(); meta != nil {
		w.maxBankStorage.UpdateBankName(bankID, meta.GetBankName())
		ack()
		return
	}

	if ts := maxBankMsg.GetTransferSummary(); ts != nil {
		amountStr := ts.GetAmount()
		amountVal, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			nack()
			return
		}

		w.maxBankStorage.UpdateMaxTransaction(bankID, ts.GetAccount(), amountVal, amountStr)
		ack()
		return
	}

	nack()
}

func (w *MaxBankWorker) handleEOF(originalMsg middleware.Message, ack, nack func()) {
	reader := w.maxBankStorage.Reader()
	batch := NewBatch(w.maxBatchWeight)
	var processedBanks []string

	for processedRecord := reader.Get(); reader.HasNext(); reader.Next() {

		maxBankResult := &protobuf.MaxBankResult{
			BankName: processedRecord.BankName,
			Account:  processedRecord.Account,
			Amount:   processedRecord.AmountString,
		}

		if batch.IsFull(maxBankResult) {
			msg, err := serializer.SerializeProtoMessage(batch.Get(), protobuf.MessageType_MAXBANK_RESULT)
			if err != nil {
				nack()
				return
			}
			if err := w.outputQueue.Send(*msg); err != nil {
				nack()
				return
			}

			// w.maxBankStorage.Flush(processedBanks)
			processedBanks = processedBanks[:0]
		}

		batch.Add(maxBankResult)
		processedBanks = append(processedBanks, processedRecord.BankID)
	}

	if !batch.isEmpty() {
		msg, err := serializer.SerializeProtoMessage(batch.Get(), protobuf.MessageType_MAXBANK_RESULT)
		if err != nil {
			nack()
			return
		}
		if err := w.outputQueue.Send(*msg); err != nil {
			nack()
			return
		}
		// w.maxBankStorage.Flush(processedBanks)
	}

	if err := w.outputQueue.Send(originalMsg); err != nil {
		nack()
		return
	}

	ack()
}
