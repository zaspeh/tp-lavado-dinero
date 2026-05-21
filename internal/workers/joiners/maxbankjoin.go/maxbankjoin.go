package maxbankjoin

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type MaxBankJoin struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
}

type JoinMaxBankConfig struct {
	InputQueueName     string
	ClientExchangeName string
	MomHost            string
	MomPort            int
}

func NewMaxBankJoin(config JoinMaxBankConfig) (*MaxBankJoin, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(config.ClientExchangeName, []string{config.ClientExchangeName}, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &MaxBankJoin{
		inputQueue:         inputQueue,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
	}, nil
}

func (j *MaxBankJoin) Run() error {
	go j.handleSignals()

	j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (j *MaxBankJoin) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MAXBANK_RESULT:
		j.sendMessage(msg, ack, nack)

	case protobuf.MessageType_EOF_:
		j.sendMessage(msg, ack, nack)

	default:
		nack()
	}
}

func (j *MaxBankJoin) handleSignals() {
	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signals

	slog.Info("shutdown signal received")

	j.inputQueue.Close()
	j.resultExchange.Close()
}

func (j *MaxBankJoin) sendMessage(msg middleware.Message, ack, nack func()) error {
	// TODO: como no hay multiclient por el momento, broadcasteo la clave
	if err := j.resultExchange.Send(msg); err != nil {
		nack()
		return err
	}

	ack()
	return nil
}
