package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/conversionjoin.go"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/maxbankjoin.go"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/scattergatherjoin"
)

func buildMaxBankJoinWorker() (workers.Worker, error) {
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

	maxBankWorkerAmount, err := getEnvIntStrict("MAX_BANK_WORKER_AMOUNT")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := maxbankjoin.JoinMaxBankConfig{
		ID:                  id,
		InputExchangeName:   inputExchangeName,
		ClientExchangeName:  clientExchangeName,
		MomHost:             host,
		MomPort:             port,
		MaxBankWorkerAmount: maxBankWorkerAmount,
	}

	return maxbankjoin.NewMaxBankJoin(config)
}

func buildMicrotransactionJoinWorker() (workers.Worker, error) {
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

	maxBatchBytes, err := getEnvIntStrict("MAX_BATCH_BYTES")
	if err != nil {
		return nil, err
	}

	id, err := getEnvStrict("ID")
	if err != nil {
		return nil, err
	}

	config := joiners.JoinMicrotransactionConfig{
		ID:                 id,
		InputExchangeName:  inputExchangeName,
		ClientExchangeName: clientExchangeName,
		MomHost:            host,
		MomPort:            port,
		MaxBatchBytes:      maxBatchBytes,
	}

	return joiners.NewJoinMicrotransaction(config)
}

func buildAvgByTypeJoinWorker() (workers.Worker, error) {
	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
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

	config := joiners.AvgByTypeJoinConfig{
		InputQueueName:     inputQueueName,
		ClientExchangeName: clientExchangeName,
		MomHost:            host,
		MomPort:            port,
		ExpectedEOFs:       expectedEOFs,
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

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	clientExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	config := conversionjoin.ConversionJoinConfig{
		InputQueueName:     inputQueueName,
		ClientExchangeName: clientExchangeName,
		MomHost:            host,
		MomPort:            port,
	}

	return conversionjoin.NewConversionJoin(config)
}

func buildScatterGatherJoinWorker() (workers.Worker, error) {

	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
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

	config := scattergatherjoin.ScatterGatherJoinConfig{
		InputQueueName:                      inputQueueName,
		ClientExchangeName:                  clientExchangeName,
		MomHost:                             host,
		MomPort:                             port,
		MaxBatchWeight:                      maxBatchWeight,
		AggregateByIntermediaryWorkerAmount: aggregateByIntermediaryWorkerAmount,
	}

	return scattergatherjoin.NewScatterGatherJoinWorker(config)
}
