package conversionjoin

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type ConversionJoinProcessor struct {
	clientResults map[string]int
	tracker      *ConversionJoinCheckpointTracker
}

func NewConversionJoinProcessor() *ConversionJoinProcessor {
	processor := &ConversionJoinProcessor{
		clientResults: make(map[string]int),
	}
	processor.tracker = NewConversionJoinCheckpointTracker(&processor.clientResults)
	return processor
}

func (p *ConversionJoinProcessor) Process(clientID string, path *protobuf.ConvertedAmount, cm *checkpoint.CheckpointManager) error {
	p.getOrCreateClient(clientID)
	p.clientResults[clientID]++

	slog.Debug("Handling ConvertedAmountBatch")

	p.tracker.MarkCountChanged(clientID)

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
	p.tracker.ClearClient(clientID)

	return uint64(totalPairs), nil
}

func (p *ConversionJoinProcessor) Cleanup(clientID string) error {
	delete(p.clientResults, clientID)
	p.tracker.ClearClient(clientID)
	return nil
}

func (p *ConversionJoinProcessor) getOrCreateClient(clientID string) {
	if _, ok := p.clientResults[clientID]; !ok {
		p.clientResults[clientID] = 0
	}
}

func (p *ConversionJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *ConversionJoinProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *ConversionJoinProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *ConversionJoinProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
