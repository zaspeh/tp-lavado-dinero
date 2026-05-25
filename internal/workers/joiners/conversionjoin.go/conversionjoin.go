package conversionjoin

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type ConversionJoinConfig struct {
	InputQueueName     string
	ClientExchangeName string
	MomHost            string
	MomPort            int
}

type ConversionJoin struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
	clientResults      map[string]int
}

func NewConversionJoin(config ConversionJoinConfig) (*ConversionJoin, error) {
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

	return &ConversionJoin{
		inputQueue:         inputQueue,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
		clientResults:      make(map[string]int),
	}, nil
}

func (j *ConversionJoin) Run() error {
	go j.handleSignals()

	err := j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	return err
}

func (j *ConversionJoin) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_CONVERTED_AMOUNT:
		j.handleConvertedAmountMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		j.HandleEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (j *ConversionJoin) handleSignals() {
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

func (j *ConversionJoin) handleConvertedAmountMessage(moneyLaundry *protobuf.MoneyLaundry, ack, _ func()) {
	clientID := moneyLaundry.GetClientID()
	j.clientResults[clientID]++
	ack()
}

func (j *ConversionJoin) HandleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, rawMsg middleware.Message, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	slog.Info("Sending EOF for client", "client_id", clientID)

	resultsCount := j.clientResults[clientID]
	resultMsg := &protobuf.ConvertedMicroPaymentResult{
		Count: int64(resultsCount),
	}
	serializedResult, err := serializer.SerializeProtoMessageWithClientID(clientID, resultMsg, protobuf.MessageType_CONVERTED_MICRO_PAYMENT_RESULT)
	if err != nil {
		nack()
		return
	}

	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	if err := j.resultExchange.SendWithKey(key, *serializedResult); err != nil {
		nack()
		return
	}

	if err := j.resultExchange.SendWithKey(key, rawMsg); err != nil {
		nack()
		return
	}

	delete(j.clientResults, clientID)
	ack()
}
