package joiners

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type JoinMicrotransaction struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
	results            map[string][]*protobuf.Microtransaction
	maxBatchBytes      int
}

type JoinMicrotransactionConfig struct {
	InputQueueName     string
	ClientExchangeName string
	MomHost            string
	MomPort            int
	MaxBatchBytes      int
}

func NewJoinMicrotransaction(config JoinMicrotransactionConfig) (*JoinMicrotransaction, error) {
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

	return &JoinMicrotransaction{
		inputQueue:         inputQueue,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
		results:            make(map[string][]*protobuf.Microtransaction),
		maxBatchBytes:      config.MaxBatchBytes,
	}, nil
}

func (j *JoinMicrotransaction) Run() error {
	go j.handleSignals()

	j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	//TODO: REVISAR SI HAY FORMA DE DEVOLVER ERRORES
	return nil
}

func (j *JoinMicrotransaction) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_MICROTRANSACTION:
		j.handleMicrotransactionMessage(moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		j.handleEOFMessage(moneyLaundry, ack, nack)

	default:
		nack()
	}
}

func (j *JoinMicrotransaction) handleSignals() {
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

func (j *JoinMicrotransaction) handleMicrotransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	microtransaction, err := serializer.DeserializeTransaction(moneyLaundry.Payload, &protobuf.Microtransaction{})
	if err != nil {
		nack()
		return
	}

	j.results[moneyLaundry.GetClientID()] = append(j.results[moneyLaundry.GetClientID()], microtransaction)
	ack()
}

func (j *JoinMicrotransaction) buildBatcher(clientID string) *batch.Batcher[*protobuf.Microtransaction, *protobuf.MicrotransactionResult] {
	sizer := protowrappers.ProtoSizer[*protobuf.Microtransaction]()
	wrapper := protowrappers.WrapMicrotransactions
	joinBatch := batch.New(j.maxBatchBytes, sizer, wrapper)
	onFlush := func(result *protobuf.MicrotransactionResult) error {
		return j.sendBatch(clientID, result)
	}
	return batch.NewBatcher(joinBatch, onFlush)
}

func (j *JoinMicrotransaction) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	clientID := moneyLaundry.GetClientID()
	results := j.results[clientID]
	batcher := j.buildBatcher(clientID)
	for _, tx := range results {
		if err := batcher.Add(tx); err != nil {
			nack()
			return
		}
	}

	if err := batcher.Flush(); err != nil {
		nack()
		return
	}

	if err := j.sendEOF(clientID); err != nil {
		nack()
		return
	}

	delete(j.results, clientID)
	ack()
}

func (j *JoinMicrotransaction) sendBatch(clientID string, batch *protobuf.MicrotransactionResult) error {
	slog.Debug("sending microtransaction batch", "client_id", clientID)
	msg, err := serializer.SerializeProtoMessageWithClientID(clientID, batch, protobuf.MessageType_MICROTRANSACTION_RESULT)
	if err != nil {
		return err
	}
	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	return j.resultExchange.SendWithKey(key, *msg)
}

func (j *JoinMicrotransaction) sendEOF(clientID string) error {
	slog.Info("sending EOF for client", "client_id", clientID)
	eof := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eof)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("%s.%s", j.clientExchangeName, clientID)
	return j.resultExchange.SendWithKey(key, msg)
}
