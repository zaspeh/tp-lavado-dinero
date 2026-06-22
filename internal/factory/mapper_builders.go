package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoextractors"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protoinserters"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor/mappers/currencyconverter"
)

func buildCurrencyConverterWorker() (workers.Worker, error) {
	apiURL, err := getEnvStrict("EXCHANGE_RATE_API_URL")
	if err != nil {
		return nil, err
	}

	maxBatchWeight, err := getEnvIntStrict("MAX_BATCH_WEIGHT")
	if err != nil {
		return nil, err
	}

	converter, err := currencyconverter.NewCurrencyConverter(apiURL)
	if err != nil {
		return nil, err
	}

	return buildStatelessWorkerInputQueueOutputQueue(
		InputQueueOutputQueueStatelessConfig[*protobuf.ToConvertTypeFilteredPayment, *protobuf.ConvertedAmount, *protobuf.ConvertedAmountBatch]{
			ReceivedMessageType: protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT_BATCH,
			Wrapper:             protowrappers.WrapConvertedAmounts,
			Extractor:           protoextractors.GetToConvertTypeFilteredPaymentItems,
			Inserter:            protoinserters.InsertConvertedAmountBatch,
			Sizer:               protowrappers.ProtoSizer[*protobuf.ConvertedAmount](),
			Processor:           currencyconverter.NewCurrencyConverterProcessor(converter),
			MaxBatchWeight:      maxBatchWeight,
		},
	)
}
