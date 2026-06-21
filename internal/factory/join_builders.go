package factory

import (
	"fmt"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/conversionjoin.go"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/scattergatherjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/maxbankjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/microtransactionjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/worker"
)

type JoinConfig struct {
	ID                 int
	ConnSettings       middleware.ConnSettings
	InputExchangeName  string
	ClientExchangeName string
	MaxBatchWeight     int
}

func buildMaxBankJoinWorker() (workers.Worker, error) {
	joinConfig, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	inputExchangeKeys := []string{
		fmt.Sprintf("%s.%s", joinConfig.InputExchangeName, strconv.Itoa(joinConfig.ID)),
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(joinConfig.InputExchangeName, inputExchangeKeys, joinConfig.ConnSettings)
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(joinConfig.ClientExchangeName, []string{joinConfig.ClientExchangeName}, joinConfig.ConnSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	newCoordinator := coordinator.NewAloneCoordinator(joinConfig.ID)

	receiver := receiver.NewSingleReceiver(inputExchange, protobuf.MessageType_MAX_BANK_RESULT_BATCH, protoextractors.GetMaxBankResultBatchItems)

	sender := sender.NewDynamicKeySender(
		resultExchange,
		func(clientID string) string {
			return fmt.Sprintf(
				"%s.%s",
				joinConfig.ClientExchangeName,
				clientID,
			)
		},
		protowrappers.WrapMaxBankResults,
		protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
		joinConfig.MaxBatchWeight,
		protoinserters.InsertMaxBankResultBatch,
	)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		return nil, err
	}

	engine := engine.NewStatefulEngine(receiver, sender, maxbankjoin.NewMaxBankJoinProcessor(), newCoordinator)
	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}

func buildMicrotransactionJoinWorker() (workers.Worker, error) {

	joinConfig, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	inputExchangeKeys := []string{
		fmt.Sprintf("%s.%s", joinConfig.InputExchangeName, strconv.Itoa(joinConfig.ID)),
	}

	inputExchange, err := middleware.CreateExchangeMiddleware(joinConfig.InputExchangeName, inputExchangeKeys, joinConfig.ConnSettings)
	if err != nil {
		return nil, err
	}

	resultExchange, err := middleware.CreateExchangeMiddleware(joinConfig.ClientExchangeName, []string{joinConfig.ClientExchangeName}, joinConfig.ConnSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	newCoordinator := coordinator.NewAloneCoordinator(joinConfig.ID)

	receiver := receiver.NewSingleReceiver(inputExchange, protobuf.MessageType_MICROTRANSACTION_BATCH, protoextractors.GetMicrotransactionBatchItems)

	sender := sender.NewDynamicKeySender(
		resultExchange,
		func(clientID string) string {
			return fmt.Sprintf(
				"%s.%s",
				joinConfig.ClientExchangeName,
				clientID,
			)
		},
		protowrappers.WrapToMicrotransactionBatch,
		protowrappers.ProtoSizer[*protobuf.Microtransaction](),
		joinConfig.MaxBatchWeight,
		protoinserters.InsertMicrotransactionBatch,
	)

	heartbeatPublisher, err := buildHeartbeatPublisher()
	if err != nil {
		return nil, err
	}

	engine := engine.NewStatefulEngine(receiver, sender, microtransactionjoin.NewMicrotransactionJoinProcessor(), newCoordinator)
	worker := worker.NewWorker(heartbeatPublisher)
	worker.AddEngine(engine)
	return worker, nil
}

func buildAvgByTypeJoinWorker() (workers.Worker, error) {
	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	clientExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	expectedEOFs, err := getEnvIntStrict("EXPECTED_EOFS")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := joiners.AvgByTypeJoinConfig{
		InputExchangeName:  inputExchangeName,
		ClientExchangeName: clientExchangeName,
		MomHost:            host,
		MomPort:            port,
		ExpectedEOFs:       expectedEOFs,
		ID:                 id,
	}

	return joiners.NewAvgByTypeJoin(config)
}

func buildConvertedMicroPaymentJoinWorker() (workers.Worker, error) {
	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	clientExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := conversionjoin.ConversionJoinConfig{
		ID:                 id,
		InputExchangeName:  inputExchangeName,
		ClientExchangeName: clientExchangeName,
		MomHost:            host,
		MomPort:            port,
	}

	return conversionjoin.NewConversionJoin(config)
}

func buildScatterGatherJoinWorker() (workers.Worker, error) {

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	clientExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	host, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	port, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	aggregateByIntermediaryWorkerAmount, err := getEnvIntStrict("AGGREGATE_BY_INTERMEDIARY_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := scattergatherjoin.ScatterGatherJoinConfig{
		ID:                                  id,
		InputExchangeName:                   inputExchangeName,
		ClientExchangeName:                  clientExchangeName,
		MomHost:                             host,
		MomPort:                             port,
		MaxBatchWeight:                      maxBatchWeight,
		AggregateByIntermediaryWorkerAmount: aggregateByIntermediaryWorkerAmount,
	}

	return scattergatherjoin.NewScatterGatherJoinWorker(config)
}

func getJoinConfig() (*JoinConfig, error) {
	mom, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	inputExchangeName, err := getEnvStrict("INPUT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	clientExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	id, err := getEnvIntStrict("ID")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	return &JoinConfig{
		ID:                 id,
		ConnSettings:       mom,
		InputExchangeName:  inputExchangeName,
		ClientExchangeName: clientExchangeName,
		MaxBatchWeight:     maxBatchWeight,
	}, nil
}
