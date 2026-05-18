package joiners

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	"google.golang.org/protobuf/proto"
)

type JoinMicrotransaction struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	mu sync.Mutex

	results []*protobuf.Microtransaction

	maxBatchTransactions int
	maxBatchBytes        int
}

type JoinMicrotransactionConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	MaxBatchTransactions int
	MaxBatchBytes        int
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

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &JoinMicrotransaction{
		inputQueue:           inputQueue,
		outputQueue:          outputQueue,
		results:              make([]*protobuf.Microtransaction, 0),
		maxBatchTransactions: config.MaxBatchTransactions,
		maxBatchBytes:        config.MaxBatchBytes,
	}, nil
}

func (j *JoinMicrotransaction) Run() {
	go j.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		j.handleMessage(msg, ack, nack)
	})

	go j.handleSignals()
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

	case protobuf.MessageType_EOF:
		j.handleEOFMessage(ack, nack)

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
	j.outputQueue.Close()
}

func (j *JoinMicrotransaction) handleMicrotransactionMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	microtransaction, err := serializer.DeserializeTransaction(moneyLaundry.Payload, &protobuf.Microtransaction{})
	if err != nil {
		nack()
		return
	}

	j.mu.Lock()
	defer j.mu.Unlock()

	j.results = append(j.results, microtransaction)

	ack()
}

func (j *JoinMicrotransaction) handleEOFMessage(ack, nack func()) {
	j.mu.Lock()

	results := j.results

	j.results = make([]*protobuf.Microtransaction, 0)

	j.mu.Unlock()

	currentBatch := make([]*protobuf.Microtransaction, 0)

	for _, transaction := range results {

		testBatch := append(currentBatch, transaction)

		testResult := &protobuf.MicrotransactionResult{
			Transactions: testBatch,
		}

		size := proto.Size(testResult)

		if len(testBatch) > j.maxBatchTransactions ||
			size > j.maxBatchBytes {

			if err := j.sendBatch(currentBatch); err != nil {
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
		if err := j.sendBatch(currentBatch); err != nil {
			nack()
			return
		}
	}

	ack()
}

func (j *JoinMicrotransaction) sendBatch(batch []*protobuf.Microtransaction) error {
	result := &protobuf.MicrotransactionResult{
		Transactions: batch,
	}

	msg, err := serializer.SerializeProtoMessage(result, protobuf.MessageType_MICROTRANSACTION_RESULT)
	if err != nil {
		return err
	}

	return j.outputQueue.Send(*msg)
}
