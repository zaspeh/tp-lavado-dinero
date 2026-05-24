package scattergatherjoin

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type ScatterGatherJoinWorker struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string

	store          *ScatterGatherStore
	maxBatchWeight int
}

type ScatterGatherJoinConfig struct {
	InputQueueName string

	ClientExchangeName string

	MomHost        string
	MomPort        int
	MaxBatchWeight int
}

func NewScatterGatherJoinWorker(config ScatterGatherJoinConfig) (*ScatterGatherJoinWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueueName,
		connSettings,
	)

	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(
		config.ClientExchangeName,
		[]string{config.ClientExchangeName},
		connSettings,
	)

	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &ScatterGatherJoinWorker{
		inputQueue:         inputQueue,
		resultExchange:     resultExchange,
		clientExchangeName: config.ClientExchangeName,
		store:              NewScatterGatherStore(),
		maxBatchWeight:     config.MaxBatchWeight,
	}, nil
}

func (sgj *ScatterGatherJoinWorker) Run() error {

	go sgj.handleSignals()

	sgj.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		sgj.handleMessage(msg, ack, nack)
	})

	//TODO: REvisar SI HAY FORMA DE RETORNAR ERRORES

	return nil
}

func (sgj *ScatterGatherJoinWorker) handleSignals() {

	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signals

	slog.Info("shutdown signal received")

	sgj.inputQueue.Close()
	sgj.resultExchange.Close()
}

func (sgj *ScatterGatherJoinWorker) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)

	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_SUSPICIOUS_PATH_BATCH:
		sgj.handleSuspiciousPathBatch(moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		slog.Info("Received EOF message in ScatterGatherJoin, forwarding to client exchange")
		sgj.handleEOF(msg, ack, nack)

	default:
		nack()
	}
}

func (sgj *ScatterGatherJoinWorker) handleSuspiciousPathBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	batchMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.SuspiciousPathBatch{})
	if err != nil {
		nack()
		return
	}

	for _, path := range batchMsg.GetPaths() {
		pair := model.OriginDestinationPair{
			Origin: model.Account{
				Bank:    path.GetOrigin().GetBank(),
				Account: path.GetOrigin().GetAccount(),
			},
			Destination: model.Account{
				Bank:    path.GetDestination().GetBank(),
				Account: path.GetDestination().GetAccount(),
			},
		}

		sgj.store.Add(pair, int(path.GetIntermediaryCount()))
	}

	ack()
}

func (sgj *ScatterGatherJoinWorker) handleEOF(msg middleware.Message, ack, nack func()) {
	if err := sgj.publishResults(); err != nil {
		nack()
		return
	}

	if err := sgj.resultExchange.Send(msg); err != nil {
		nack()
		return
	}

	ack()
}

func (sgj *ScatterGatherJoinWorker) publishResults() error {
	/*defer sgj.store.Clear()

	b := batch.New(sgj.maxBatchWeight, protowrappers.ProtoSizer[]())*/
	return nil

}
