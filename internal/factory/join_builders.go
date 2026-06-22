package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/avgbytypejoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/conversionjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/maxbankjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/microtransactionjoin"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/joiners/scattergatherjoin"
)

type joinConfig struct {
	ID                 int
	ConnSettings       middleware.ConnSettings
	InputExchangeName  string
	ClientExchangeName string
	MaxBatchWeight     int
}

func buildMaxBankJoinWorker() (workers.Worker, error) {
	cfg, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	return buildJoinWorker(joinWorkerConfig[*protobuf.MaxBankResult, *protobuf.MaxBankResult, *protobuf.MaxBankResultBatch]{
		Mom:            cfg.ConnSettings,
		ID:             cfg.ID,
		InputExchange:  cfg.InputExchangeName,
		ClientExchange: cfg.ClientExchangeName,
		MaxBatchWeight: cfg.MaxBatchWeight,
		ReceivedType:   protobuf.MessageType_MAX_BANK_RESULT_BATCH,
		ExtractItems:   protoextractors.GetMaxBankResultBatchItems,
		Processor:      maxbankjoin.NewMaxBankJoinProcessor(),
		Wrapper:        protowrappers.WrapMaxBankResults,
		Sizer:          protowrappers.ProtoSizer[*protobuf.MaxBankResult](),
		Inserter:       protoinserters.InsertMaxBankResultBatch,
	})
}

func buildMicrotransactionJoinWorker() (workers.Worker, error) {
	cfg, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	return buildJoinWorker(joinWorkerConfig[*protobuf.Microtransaction, *protobuf.Microtransaction, *protobuf.MicrotransactionBatch]{
		Mom:            cfg.ConnSettings,
		ID:             cfg.ID,
		InputExchange:  cfg.InputExchangeName,
		ClientExchange: cfg.ClientExchangeName,
		MaxBatchWeight: cfg.MaxBatchWeight,
		ReceivedType:   protobuf.MessageType_MICROTRANSACTION_BATCH,
		ExtractItems:   protoextractors.GetMicrotransactionBatchItems,
		Processor:      microtransactionjoin.NewMicrotransactionJoinProcessor(),
		Wrapper:        protowrappers.WrapToMicrotransactionBatch,
		Sizer:          protowrappers.ProtoSizer[*protobuf.Microtransaction](),
		Inserter:       protoinserters.InsertMicrotransactionBatch,
	})
}

func buildAvgByTypeJoinWorker() (workers.Worker, error) {
	cfg, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	return buildJoinWorker(joinWorkerConfig[*protobuf.AvgByTypeResult, *protobuf.AvgByTypeResult, *protobuf.AvgByTypeResultBatch]{
		Mom:            cfg.ConnSettings,
		ID:             cfg.ID,
		InputExchange:  cfg.InputExchangeName,
		ClientExchange: cfg.ClientExchangeName,
		MaxBatchWeight: cfg.MaxBatchWeight,
		ReceivedType:   protobuf.MessageType_AVGBYTYPE_RESULT_BATCH,
		ExtractItems:   protoextractors.GetAvgByTypeResultBatchItems,
		Processor:      avgbytypejoin.NewAvgByTypeJoinProcessor(),
		Wrapper:        protowrappers.WrapAvgByTypeResults,
		Sizer:          protowrappers.ProtoSizer[*protobuf.AvgByTypeResult](),
		Inserter:       protoinserters.InsertAvgByTypeResultBatch,
	})
}

func buildConvertedMicroPaymentJoinWorker() (workers.Worker, error) {
	cfg, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	return buildJoinWorker(joinWorkerConfig[*protobuf.ConvertedAmount, *protobuf.ConvertedMicroPaymentResult, *protobuf.ConvertedMicroPaymentResultBatch]{
		Mom:            cfg.ConnSettings,
		ID:             cfg.ID,
		InputExchange:  cfg.InputExchangeName,
		ClientExchange: cfg.ClientExchangeName,
		MaxBatchWeight: cfg.MaxBatchWeight,
		ReceivedType:   protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		ExtractItems:   protoextractors.GetConvertedAmountBatchItems,
		Processor:      conversionjoin.NewConversionJoinProcessor(),
		Wrapper:        protowrappers.WrapToConvertedMicropaymentResultBatch,
		Sizer:          protowrappers.ProtoSizer[*protobuf.ConvertedMicroPaymentResult](),
		Inserter:       protoinserters.InsertConvertedMicropaymentResultBatch,
	})
}

func buildScatterGatherJoinWorker() (workers.Worker, error) {
	cfg, err := getJoinConfig()
	if err != nil {
		return nil, err
	}

	return buildJoinWorker(joinWorkerConfig[*protobuf.SuspiciousPath, *protobuf.Account, *protobuf.SuspiciousAccountBatch]{
		Mom:            cfg.ConnSettings,
		ID:             cfg.ID,
		InputExchange:  cfg.InputExchangeName,
		ClientExchange: cfg.ClientExchangeName,
		MaxBatchWeight: cfg.MaxBatchWeight,
		ReceivedType:   protobuf.MessageType_SUSPICIOUS_PATH_BATCH,
		ExtractItems:   protoextractors.GetSuspiciousPathBatchItems,
		Processor:      scattergatherjoin.NewScatterGatherJoinProcessor(),
		Wrapper:        protowrappers.WrapToSuspiciousAccountBatch,
		Sizer:          protowrappers.ProtoSizer[*protobuf.Account](),
		Inserter:       protoinserters.InsertSuspiciousAccountBatch,
	})
}

func getJoinConfig() (*joinConfig, error) {
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

	return &joinConfig{
		ID:                 id,
		ConnSettings:       mom,
		InputExchangeName:  inputExchangeName,
		ClientExchangeName: clientExchangeName,
		MaxBatchWeight:     maxBatchWeight,
	}, nil
}