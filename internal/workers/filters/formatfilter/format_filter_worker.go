package formatfilter

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type FormatFilterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	AllowedFormats  []string
}

type FormatFilterWorker struct {
	inputQueue    middleware.Middleware
	outputQuueue  middleware.Middleware
	alloweFormats []string
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
		inputQueue:    inputQueue,
		outputQuueue:  outputQueue,
		alloweFormats: cfg.AllowedFormats,
	}, nil
}

func (w *FormatFilterWorker) Run() error {
	return nil
}
