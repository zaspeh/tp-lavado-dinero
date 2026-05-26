package scattergatherjoin

import (
	"fmt"
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

type ScatterGatherJoinWorker struct {
	inputQueue         middleware.Middleware
	resultExchange     *middleware.ExchangeMiddleware
	clientExchangeName string
	stores             map[string]*ScatterGatherStore
	storesMu           sync.RWMutex
	maxBatchWeight     int
	targetEofCount     int
	clientEOFs         map[string]int
}

type ScatterGatherJoinConfig struct {
	InputQueueName                      string
	ClientExchangeName                  string
	MomHost                             string
	MomPort                             int
	MaxBatchWeight                      int
	AggregateByIntermediaryWorkerAmount int
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
		stores:             make(map[string]*ScatterGatherStore),
		maxBatchWeight:     config.MaxBatchWeight,
		targetEofCount:     config.AggregateByIntermediaryWorkerAmount,
		clientEOFs:         make(map[string]int),
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
		sgj.handleEOF(moneyLaundry, msg, ack, nack)

	default:
		nack()
	}
}

func (sgj *ScatterGatherJoinWorker) handleSuspiciousPathBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	batchMsg, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.SuspiciousPathBatch{})
	store := sgj.getStore(moneyLaundry.GetClientID())
	if err != nil {
		nack()
		return
	}
	slog.Debug("Handling SuspiciousPathBatch")

	for _, path := range batchMsg.GetPaths() {
		slog.Debug("Handling SuspiciousPathBatch Origin", path.GetOrigin().GetBank(), path.GetOrigin().GetAccount(), "Destination", path.GetDestination().GetBank(), path.GetDestination().GetAccount())
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

		store.Add(pair, int(path.GetIntermediaryCount()))
	}

	ack()
}

func (sgj *ScatterGatherJoinWorker) handleEOF(msg *protobuf.MoneyLaundry, rawMsg middleware.Message, ack, nack func()) {
	// Logica agregada por Andres segun entendioa las 2 am
	// TODO: borrar comentario
	// ----------------------
	slog.Debug("Handling EOF")
	clientID := msg.GetClientID()
	slog.Debug("Handling EOF for client", clientID)
	clientEOFCount, ok := sgj.clientEOFs[clientID]
	if !ok {
		clientEOFCount = 0
	}
	clientEOFCount++
	slog.Debug("client EOF count", clientID)
	sgj.clientEOFs[clientID] = clientEOFCount
	if !(clientEOFCount >= sgj.targetEofCount) {
		ack()
		return
	}
	// --------------------

	if err := sgj.publishResults(clientID); err != nil {
		nack()
		return
	}

	slog.Debug(
		"sending EOF",
	)
	slog.Info("sending EOF for client", "client_id", clientID)
	eofMsg := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{},
	}

	serializeMsg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMsg)
	if err != nil {
		nack()
		return
	}

	publishKey := fmt.Sprintf("%s.%s", sgj.clientExchangeName, clientID)
	if err := sgj.resultExchange.SendWithKey(publishKey, serializeMsg); err != nil {
		nack()
		return
	}

	ack()
}

func (sgj *ScatterGatherJoinWorker) publishResults(clientID string) error {
	slog.Debug("Publishing Results")
	store := sgj.getStore(clientID)
	defer func() {
		store.Clear()

		sgj.storesMu.Lock()
		delete(sgj.stores, clientID)
		sgj.storesMu.Unlock()
	}()

	suspiciousAccounts := make(map[model.Account]struct{})

	publishKey := fmt.Sprintf("%s.%s", sgj.clientExchangeName, clientID)

	for pair, count := range store.GetPaths() {

		if count < 5 {
			continue
		}

		origin := pair.Origin

		suspiciousAccounts[origin] = struct{}{}
	}

	b := batch.New(
		sgj.maxBatchWeight,
		protowrappers.ProtoSizer[*protobuf.Account](),
		protowrappers.WrapSuspiciousAccounts,
	)

	batcher := batch.NewBatcher(
		b,
		func(pb *protobuf.SuspiciousAccountBatch) error {

			serializedMsg, err := serializer.SerializeProtoMessage(
				pb,
				protobuf.MessageType_SUSPICIOUS_ACCOUNT_BATCH,
			)

			if err != nil {
				return err
			}

			return sgj.resultExchange.SendWithKey(publishKey, *serializedMsg)
		},
	)

	for account := range suspiciousAccounts {
		slog.Debug(
			"adding suspicious account to response",
			"account", account.GetAccount(),
			"bank", account.GetBank(),
		)
		protoAccount := &protobuf.Account{
			Bank:    account.GetBank(),
			Account: account.GetAccount(),
		}

		if err := batcher.Add(protoAccount); err != nil {
			return err
		}
	}

	slog.Debug(
		"flushing",
	)
	return batcher.Flush()
}

func (sgj *ScatterGatherJoinWorker) getStore(
	clientID string,
) *ScatterGatherStore {

	sgj.storesMu.Lock()
	defer sgj.storesMu.Unlock()

	store, ok := sgj.stores[clientID]

	if !ok {
		store = NewScatterGatherStore()
		sgj.stores[clientID] = store
	}

	return store
}
