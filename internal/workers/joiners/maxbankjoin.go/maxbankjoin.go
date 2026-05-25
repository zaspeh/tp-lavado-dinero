package maxbankjoin

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

type MaxBankJoin struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
	targetEofCount     int
	clientEOFs         map[string]int
}

type JoinMaxBankConfig struct {
	InputQueueName      string
	ClientExchangeName  string
	MomHost             string
	MomPort             int
	MaxBankWorkerAmount int
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
		clientEOFs:         make(map[string]int),
		targetEofCount:     config.MaxBankWorkerAmount,
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
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MAXBANK_RESULT:
		j.sendMessage(moneyLaundry, msg, ack, nack)
	case protobuf.MessageType_EOF_:
		j.handleEOFMessage(moneyLaundry, ack, nack)
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

func (j *MaxBankJoin) sendMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) error {
	clientID := moneyLaundry.GetClientID()
	publishKey := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)

	if err := j.resultExchange.SendWithKey(publishKey, msg); err != nil {
		nack()
		return err
	}

	ack()
	return nil
}

func (j *MaxBankJoin) handleEOFMessage(msg *protobuf.MoneyLaundry, ack, nack func()) {
	slog.Info("Received EOF message, forwarding to client exchange")
	clientID := msg.GetClientID()
	clientEOFCount, ok := j.clientEOFs[clientID]
	if !ok {
		clientEOFCount = 0
	}
	clientEOFCount++
	j.clientEOFs[clientID] = clientEOFCount

	if clientEOFCount >= j.targetEofCount {
		eofMsg := &protobuf.MoneyLaundry_EofMessage{
			EofMessage: &protobuf.EOF{},
		}

		serializeMsg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMsg)
		if err != nil {
			nack()
			return
		}

		publishKey := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
		if err := j.resultExchange.SendWithKey(publishKey, serializeMsg); err != nil {
			nack()
			return
		}
	}
	ack()
}
