package formatfilter

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type FormatFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AllowedFormats  []string
}

type FormatFilterWorker struct {
	inputQueue     middleware.Middleware
	outputQueue    middleware.Middleware
	allowedFormats []string
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

	return &FormatFilterWorker{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		allowedFormats: cfg.AllowedFormats,
	}, nil
}

func (w *FormatFilterWorker) Run() error {
	go w.handleSignals()

	w.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		// Aquí iría la lógica para procesar el mensaje y filtrar por formato
		// Por ejemplo:
		// 1. Deserializar el mensaje
		// 2. Verificar si el formato del pago está en w.allowedFormats
		// 3. Si es válido, enviar el mensaje a w.outputQueue
		// 4. Acknowledge o Nack dependiendo del resultado
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
