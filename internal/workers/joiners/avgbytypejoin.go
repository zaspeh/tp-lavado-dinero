package joiners

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeJoin struct {
	inputQueue     middleware.Middleware
	resultExchange *middleware.ExchangeMiddleware

	expectedEOFs int
	receivedEOFs map[string]int
}

type AvgByTypeJoinConfig struct {
	InputQueueName     string
	ClientExchangeName string

	MomHost string
	MomPort int

	ExpectedEOFs int
}

func NewAvgByTypeJoin(config AvgByTypeJoinConfig) (*AvgByTypeJoin, error) {
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

	return &AvgByTypeJoin{
		inputQueue:     inputQueue,
		resultExchange: resultExchange,
		expectedEOFs:   config.ExpectedEOFs,
		receivedEOFs:   make(map[string]int),
	}, nil
}

func (j *AvgByTypeJoin) Run() error {
	go j.handleSignals()

	j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	return nil
}

func (j *AvgByTypeJoin) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {

	case protobuf.MessageType_AVGBYTYPE_RESULT:
		j.handleResult(moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		j.handleEOF(moneyLaundry, ack, nack)

	default:
		nack()
	}
}

func (j *AvgByTypeJoin) handleResult(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	result, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.AvgByTypeResult{})
	if err != nil {
		nack()
		return
	}

	if err := j.sendResult(result); err != nil {
		nack()
		return
	}

	ack()
}

func (j *AvgByTypeJoin) handleEOF(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()

	j.receivedEOFs[clientID]++

	slog.Info(
		"received EOF from avg by type worker",
		"clientID", clientID,
		"count", j.receivedEOFs[clientID],
	)

	if j.receivedEOFs[clientID] < j.expectedEOFs {
		ack()
		return
	}

	delete(j.receivedEOFs, clientID)

	if err := j.sendEOF(clientID); err != nil {
		nack()
		return
	}

	ack()
}

func (j *AvgByTypeJoin) sendResult(result *protobuf.AvgByTypeResult) error {
	msg, err := serializer.SerializeProtoMessage(result, protobuf.MessageType_AVGBYTYPE_RESULT)
	if err != nil {
		return err
	}

	return j.resultExchange.Send(*msg)
}

func (j *AvgByTypeJoin) sendEOF(clientID string) error {
	eof := &protobuf.EOF{
		ClientID: clientID,
	}

	msg, err := serializer.SerializeProtoMessage(eof, protobuf.MessageType_EOF_)
	if err != nil {
		return err
	}

	return j.resultExchange.Send(*msg)
}

func (j *AvgByTypeJoin) handleSignals() {
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
