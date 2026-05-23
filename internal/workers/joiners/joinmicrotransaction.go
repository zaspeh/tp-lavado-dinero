package joiners

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	"google.golang.org/protobuf/proto"
)

type JoinMicrotransaction struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string

	results map[string][]*protobuf.Microtransaction

	maxBatchTransactions int
	maxBatchBytes        int
}

type JoinMicrotransactionConfig struct {
	InputQueueName     string
	ClientExchangeName string

	MomHost string
	MomPort int

	MaxBatchTransactions int
	MaxBatchBytes        int // probar con un mega
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
		inputQueue:           inputQueue,
		resultExchange:       resultExchange,
		clientExchangeName:   config.ClientExchangeName,
		results:              make(map[string][]*protobuf.Microtransaction),
		maxBatchTransactions: config.MaxBatchTransactions,
		maxBatchBytes:        config.MaxBatchBytes,
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
	slog.Info(
		"join received microtransaction",
		"client", moneyLaundry.GetClientID(),
	)

	microtransaction, err := serializer.DeserializeTransaction(moneyLaundry.Payload, &protobuf.Microtransaction{})
	if err != nil {
		nack()
		return
	}

	j.results[moneyLaundry.GetClientID()] = append(j.results[moneyLaundry.GetClientID()], microtransaction)

	slog.Info(
		"join acking microtransaction",
		"client", moneyLaundry.GetClientID(),
	)

	ack()
}

func (j *JoinMicrotransaction) handleEOFMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	slog.Info(
		"join received EOF",
		"client", moneyLaundry.GetClientID(),
	)

	clientID := moneyLaundry.GetClientID()
	results := j.results[clientID]

	slog.Info("processing EOF", "client_id", clientID)

	if len(results) == 0 {
		ack()
		return
	}

	delete(j.results, clientID)

	currentBatch := make([]*protobuf.Microtransaction, 0)

	for _, transaction := range results {

		testBatch := append(currentBatch, transaction)

		testResult := &protobuf.MicrotransactionResult{
			Transactions: testBatch,
		}

		size := proto.Size(testResult)

		if len(testBatch) > j.maxBatchTransactions ||
			size > j.maxBatchBytes {

			if err := j.sendBatch(clientID, currentBatch); err != nil {
				nack()
				return
			}

			currentBatch = []*protobuf.Microtransaction{
				transaction,
			}

			continue
		}

		currentBatch = testBatch
	}

	if len(currentBatch) > 0 {
		if err := j.sendBatch(clientID, currentBatch); err != nil {
			nack()
			return
		}
	}

	if err := j.sendEOF(clientID); err != nil {
		nack()
		return
	}

	ack()
}

func (j *JoinMicrotransaction) sendBatch(clientID string, batch []*protobuf.Microtransaction) error {
	slog.Info("sending microtransaction batch", "client_id", clientID, "batch_size", len(batch))

	result := &protobuf.MicrotransactionResult{
		Transactions: batch,
	}

	msg, err := serializer.SerializeProtoMessage(result, protobuf.MessageType_MICROTRANSACTION_RESULT)
	if err != nil {
		return err
	}

	return j.resultExchange.Send(*msg) // más adelante tener en cuenta el clientID
}

func (j *JoinMicrotransaction) sendEOF(clientID string) error {
	eof := &protobuf.EOF{
		ClientID: clientID,
	}

	msg, err := serializer.SerializeProtoMessage(eof, protobuf.MessageType_EOF_)
	if err != nil {
		return err
	}

	return j.resultExchange.Send(*msg)
}
