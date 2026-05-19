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
}

type MaxBankWorkerConfig struct {
	MomHost           string
	MomPort           int
	InputExchangeName string
	InputKeys         []string
	OutputQueueName   string
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
	results := w.maxBankStorage.GetResults()
	for _, rec := range results {
		mb := &protobuf.MaxBank{
			FromBank: rec.BankName,
			Payload: &protobuf.MaxBank_TransferSummary{
				TransferSummary: &protobuf.TransferSummary{
					Account: rec.Account,
					Amount:  rec.AmountString,
				},
			},
		}

		serialized, err := serializer.SerializeProtoMessage(mb, protobuf.MessageType_MAXBANK)
		if err != nil {
			nack()
			return
		}

		if err := w.outputQueue.Send(*serialized); err != nil {
			nack()
			return
		}
	}

	if err := w.outputQueue.Send(originalMsg); err != nil {
		nack()
		return
	}

	ack()
}
