package joiners

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeJoin struct {
	inputExchange    *middleware.ExchangeMiddleware
	resultExchange  *middleware.ExchangeMiddleware

	expectedEOFs int
	receivedEOFs map[string]int

	clientExchangeName string
}

type AvgByTypeJoinConfig struct {
	InputExchangeName  string
	ClientExchangeName string

	MomHost string
	MomPort int

	ExpectedEOFs int
	ID         string
}

func NewAvgByTypeJoin(config AvgByTypeJoinConfig) (*AvgByTypeJoin, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputExchangeKeys := []string{
		fmt.Sprintf("%s.%s", config.InputExchangeName, config.ID),
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, inputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(config.ClientExchangeName, []string{config.ClientExchangeName}, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &AvgByTypeJoin{
		inputExchange:     inputExchange,
		resultExchange:    resultExchange,
		clientExchangeName: config.ClientExchangeName,
		expectedEOFs:      config.ExpectedEOFs,
		receivedEOFs:      make(map[string]int),
	}, nil
}

func (j *AvgByTypeJoin) Run() error {
	go j.handleSignals()

	j.inputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
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

	case protobuf.MessageType_AVGBYTYPE_RESULT_BATCH:
		j.handleResult(msg, moneyLaundry.GetClientID(), ack, nack)

	case protobuf.MessageType_EOF_:
		j.handleEOF(moneyLaundry, ack, nack)

	default:
		nack()
	}
}

func (j *AvgByTypeJoin) handleResult(msg middleware.Message, clientID string, ack, nack func()) {
	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	if err := j.resultExchange.SendWithKey(key, msg); err != nil {
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

	slog.Info("Received all EOFs for client, forwarding EOF", "clientID", clientID)

	delete(j.receivedEOFs, clientID)

	if err := j.sendEOF(clientID); err != nil {
		nack()
		return
	}

	ack()
}

func (j *AvgByTypeJoin) sendEOF(clientID string) error {
	eof := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eof, "")
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	return j.resultExchange.SendWithKey(key, msg)
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
	j.inputExchange.Close()
	j.resultExchange.Close()
}
