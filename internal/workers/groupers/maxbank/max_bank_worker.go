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
	reader := w.maxBankStorage.Reader()

	var currentBatch []*protobuf.MaxBank
	var processedBanks []string

	for processedBanks := reader.Get(); reader.HasNext(); reader.Next() {
		enriched := reader.Get()

		// 1. Transformar y acumular
		pb := w.toProto(enriched)
		currentBatch = append(currentBatch, pb)

		// Tracking: Guardamos qué bancos estamos metiendo en este lote
		processedBanks = append(processedBanks, enriched.BankID)

		// 2. Lógica de Flush (Batching)
		if w.shouldFlush(currentBatch) {
			if err := w.send(currentBatch); err != nil {
				nack() // Si falla la red, NO borramos memoria
				return
			}

			// 3. Éxito: Liberar memoria de lo enviado
			w.storage.Commit(processedBanks)

			// Reset de lotes
			currentBatch = nil
			processedBanks = nil
		}

		reader.Next()
	}

	// Procesar remanente final...
	// Enviar EOF original...
	ack()
}
