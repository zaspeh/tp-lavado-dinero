package conversionjoin

import (
	"encoding/json"
	"fmt"
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type ConversionJoinProcessor struct {
	clientResults map[string]int
}

type countEntity struct {
	Count int `json:"count"`
}

func NewConversionJoinProcessor() *ConversionJoinProcessor {
	return &ConversionJoinProcessor{
		clientResults: make(map[string]int),
	}
}

func (p *ConversionJoinProcessor) Process(clientID string, path *protobuf.ConvertedAmount, cm *checkpoint.CheckpointManager) error {
	p.clientResults[clientID]++

	slog.Debug("Handling ConvertedAmountBatch")

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "count")
	}

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

func (p *ConversionJoinProcessor) ListEntities(clientID string) ([]string, error) {
	if _, ok := p.clientResults[clientID]; !ok {
		return nil, nil
	}
	return []string{"count"}, nil
}

func (p *ConversionJoinProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	if entityID != "count" {
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}

	count := p.clientResults[clientID]
	return json.Marshal(countEntity{Count: count})
}

func (p *ConversionJoinProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	if entityID != "count" {
		return fmt.Errorf("unknown entity: %s", entityID)
	}

	var entity countEntity
	if err := json.Unmarshal(data, &entity); err != nil {
		return err
	}

	p.clientResults[clientID] = entity.Count
	return nil
}

func (p *ConversionJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
