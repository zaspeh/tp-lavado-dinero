package aggregatebyintermediary

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AggregateByIntermediaryWorker struct {
	originInputExchange      *middleware.ExchangeMiddleware
	destinationInputExchange *middleware.ExchangeMiddleware

	outputQueue middleware.Middleware

	store                      *IntermediaryStore
	eofMu                      sync.Mutex
	eofReceivedFromOrigin      bool
	eofReceivedFromDestination bool

	maxBatchWeight int
}

type AggregateByIntermediaryWorkerConfig struct {
	ID                           string
	MomHost                      string
	MomPort                      int
	OriginInputExchangeName      string
	DestinationInputExchangeName string
	OutputQueueName              string
	MaxBatchWeight               int
}

func NewAggregateByIntermediaryWorker(config AggregateByIntermediaryWorkerConfig) (*AggregateByIntermediaryWorker, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	originInputExchangeKeys := []string{config.OriginInputExchangeName + "." + config.ID}
	originInputExchange, err := middleware.CreateExchangeMiddleware(config.OriginInputExchangeName, originInputExchangeKeys, connSettings)
	if err != nil {
		return nil, err
	}

	destinationInputExchangeKeys := []string{config.DestinationInputExchangeName + "." + config.ID}
	destinationInputExchange, err := middleware.CreateExchangeMiddleware(config.DestinationInputExchangeName, destinationInputExchangeKeys, connSettings)
	if err != nil {
		originInputExchange.Close()
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		originInputExchange.Close()
		destinationInputExchange.Close()
		return nil, err
	}

	return &AggregateByIntermediaryWorker{
		originInputExchange:      originInputExchange,
		destinationInputExchange: destinationInputExchange,
		outputQueue:              outputQueue,
		store:                    NewIntermediaryStore(),
		maxBatchWeight:           config.MaxBatchWeight,
	}, nil
}

func (abi *AggregateByIntermediaryWorker) Run() error {
	go abi.handleSignals()

	go abi.originInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		abi.handleOriginMessage(msg, ack, nack)
	})

	abi.destinationInputExchange.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		abi.handleDestinationMessage(msg, ack, nack)
	})

	return nil
}

func (abi *AggregateByIntermediaryWorker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)
	<-signals
	slog.Info("shutdown signal received")
	abi.originInputExchange.Close()
	abi.destinationInputExchange.Close()
	abi.outputQueue.Close()
}

func (abi *AggregateByIntermediaryWorker) handleOriginMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_INTERMEDIARYPAIR:
		abi.handleOriginIntermediaryPairMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		abi.handleOriginEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (abi *AggregateByIntermediaryWorker) handleDestinationMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_INTERMEDIARYPAIR:
		abi.handleDestinationIntermediaryPairMessage(moneyLaundry, ack, nack)
	case protobuf.MessageType_EOF_:
		abi.handleDestinationEOFMessage(moneyLaundry, msg, ack, nack)
	default:
		nack()
	}
}

func (abi *AggregateByIntermediaryWorker) handleOriginIntermediaryPairMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	intermediaryPairMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.IntermediaryPair{})
	if err != nil {
		nack()
		return
	}

	origin := model.Account{
		Bank:    intermediaryPairMsg.GetAccount().GetBank(),
		Account: intermediaryPairMsg.GetAccount().GetAccount(),
	}

	intermediary := model.Account{
		Bank:    intermediaryPairMsg.GetIntermediary().GetBank(),
		Account: intermediaryPairMsg.GetIntermediary().GetAccount(),
	}

	abi.store.AddOrigin(intermediary, origin)

	ack()
}

func (abi *AggregateByIntermediaryWorker) handleDestinationIntermediaryPairMessage(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	intermediaryPairMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.IntermediaryPair{})
	if err != nil {
		nack()
		return
	}

	destination := model.Account{
		Bank:    intermediaryPairMsg.GetAccount().GetBank(),
		Account: intermediaryPairMsg.GetAccount().GetAccount(),
	}

	intermediary := model.Account{
		Bank:    intermediaryPairMsg.GetIntermediary().GetBank(),
		Account: intermediaryPairMsg.GetIntermediary().GetAccount(),
	}

	abi.store.AddDestination(intermediary, destination)

	ack()
}

func (abi *AggregateByIntermediaryWorker) handleOriginEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	abi.eofMu.Lock()
	defer abi.eofMu.Unlock()

	abi.eofReceivedFromOrigin = true

	if !abi.eofReceivedFromDestination {
		ack()
		return
	}

	totalPairs, err := abi.publishPairs(moneyLaundry.GetClientID())
	if err != nil {
		nack()
		return
	}

	slog.Debug("Creating new EOF")

	innerMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: uint64(totalPairs),
		},
	}

	eofMsg, err := protobuf.SerializeProtoMessageONTRIAL(moneyLaundry.GetClientID(), protobuf.MessageType_EOF_, innerMessage)
	if err != nil {
		nack()
		return
	}

	slog.Debug(
		"Forwarding EOF",
		"Pairs",
		totalPairs,
	)

	if err := abi.outputQueue.Send(eofMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (abi *AggregateByIntermediaryWorker) handleDestinationEOFMessage(moneyLaundry *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	abi.eofMu.Lock()
	defer abi.eofMu.Unlock()

	abi.eofReceivedFromDestination = true

	if !abi.eofReceivedFromOrigin {
		ack()
		return
	}

	totalPairs, err := abi.publishPairs(moneyLaundry.GetClientID())
	if err != nil {
		nack()
		return
	}

	slog.Debug("Creating new EOF")

	innerMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: uint64(totalPairs),
		},
	}

	eofMsg, err := protobuf.SerializeProtoMessageONTRIAL(moneyLaundry.GetClientID(), protobuf.MessageType_EOF_, innerMessage)
	if err != nil {
		nack()
		return
	}

	slog.Debug(
		"Forwarding EOF",
		"Pairs",
		totalPairs,
	)

	if err := abi.outputQueue.Send(eofMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (abi *AggregateByIntermediaryWorker) publishPairs(clientID string) (uint64, error) {
	defer abi.store.Clear()

	totalPairs := 0

	b := batch.New(
		abi.maxBatchWeight,
		protowrappers.ProtoSizer[*protobuf.SuspiciousPath](),
		protowrappers.WrapSuspiciousPaths,
	)

	batcher := batch.NewBatcher(b, func(pb *protobuf.SuspiciousPathBatch) error {

		serializedMsg, err := serializer.SerializeProtoMessageWithClientID(clientID, pb, protobuf.MessageType_SUSPICIOUS_PATH_BATCH)
		if err != nil {
			return err
		}

		return abi.outputQueue.Send(*serializedMsg)
	})

	for pair, intermediaryCount := range abi.store.GetPairs() {
		path := &protobuf.SuspiciousPath{
			Origin: &protobuf.Account{
				Bank:    pair.Origin.Bank,
				Account: pair.Origin.Account,
			},

			Destination: &protobuf.Account{
				Bank:    pair.Destination.Bank,
				Account: pair.Destination.Account,
			},

			IntermediaryCount: uint32(intermediaryCount),
		}
		totalPairs++

		if err := batcher.Add(path); err != nil {
			return 0, err
		}
	}

	return uint64(totalPairs), batcher.Flush()
}
