package conversionjoin

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type ConversionJoinProcessor struct {
	clientResults map[string]int
}

func NewConversionJoinProcessor() *ConversionJoinProcessor {
	return &ConversionJoinProcessor{
		clientResults: make(map[string]int),
	}
}

func (p *ConversionJoinProcessor) Process(clientID string, path *protobuf.ConvertedAmount) error {
	p.clientResults[clientID]++

	slog.Debug("Handling ConvertedAmountBatch")

	return nil
}

func (p *ConversionJoinProcessor) Finalize(clientID string, yield func(result *protobuf.ConvertedMicroPaymentResult) error) (uint64, error) {
	result := p.clientResults[clientID]

	totalPairs := 1

	response := &protobuf.ConvertedMicroPaymentResult{
		Count: int64(result),
	}

	slog.Debug("Saving response", "count", response.Count)

	if err := yield(response); err != nil {
		return 0, nil
	}
	delete(p.clientResults, clientID)

	return uint64(totalPairs), nil
}

func (p *ConversionJoinProcessor) Cleanup(clientID string) error {
	delete(p.clientResults, clientID)
	return nil
}
