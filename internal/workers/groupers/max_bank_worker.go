package groupers

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

type MaxBankRecord struct {
	Account      string
	AmountValue  float64
	AmountString string
}

type MaxBankWorker struct {
	inputQueue      middleware.Middleware
	outputQueue     middleware.Middleware
	bankNames       map[string]string
	maxTransactions map[string]MaxBankRecord
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
		inputQueue:      inputExchange,
		outputQueue:     outputQueue,
		bankNames:       make(map[string]string),
		maxTransactions: make(map[string]MaxBankRecord),
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
		w.bankNames[bankID] = meta.GetBankName()
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

		current, ok := w.maxTransactions[bankID]
		if !ok || amountVal > current.AmountValue {
			w.maxTransactions[bankID] = MaxBankRecord{
				Account:      ts.GetAccount(),
				AmountValue:  amountVal,
				AmountString: amountStr,
			}
		}

		ack()
		return
	}

	nack()
}

func (w *MaxBankWorker) handleEOF(originalMsg middleware.Message, ack, nack func()) {
	results := make(map[string]MaxBankRecord, len(w.maxTransactions))
	for k, v := range w.maxTransactions {
		results[k] = v
	}

	for bankID, rec := range results {
		mb := &protobuf.MaxBank{
			FromBank: bankID,
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
