package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/joiners/maxbankjoin.go"
)

func buildMaxBankJoinWorker() (workers.Worker, error) {
	inputQueueName, err := getEnvStrict("INPUT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	clienExchangeName, err := getEnvStrict("CLIENT_EXCHANGE_NAME")
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

	config := maxbankjoin.JoinMaxBankConfig{
		InputQueueName:     inputQueueName,
		ClientExchangeName: clienExchangeName,
		MomHost:            host,
		MomPort:            port,
	}

	return maxbankjoin.NewMaxBankJoin(config)
}

func buildMicrotransactionJoinWorker() (workers.Worker, error) {
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

	maxBatchTransactions, err := getEnvIntStrict("MAX_BATCH_TRANSACTIONS")
	if err != nil {
		return nil, err
	}

	maxBatchBytes, err := getEnvIntStrict("MAX_BATCH_BYTES")
	if err != nil {
		return nil, err
	}

	config := joiners.JoinMicrotransactionConfig{
		InputQueueName:       inputQueueName,
		ClientExchangeName:   clientExchangeName,
		MomHost:              host,
		MomPort:              port,
		MaxBatchTransactions: maxBatchTransactions,
		MaxBatchBytes:        maxBatchBytes,
	}

	return joiners.NewJoinMicrotransaction(config)
}
