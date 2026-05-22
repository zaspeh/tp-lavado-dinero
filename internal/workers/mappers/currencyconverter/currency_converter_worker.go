package currencyconverter

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type CurrencyConverterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	Converter       Converter
}

type CurrencyConverterWorker struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware
	converter   Converter
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

	return &CurrencyConverterWorker{
		inputQueue:  inputQueue,
		outputQueue: outputQueue,
		converter:   cfg.Converter,
	}, nil
}

func (w *CurrencyConverterWorker) Run() error {
	return nil
}
