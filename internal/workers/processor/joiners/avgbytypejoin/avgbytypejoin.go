package avgbytypejoin

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type AvgByTypeJoinProcessor struct {
	stores  map[string]*AvgByTypeResultStore
	tracker *AvgByTypeJoinCheckpointTracker
}

func NewAvgByTypeJoinProcessor() *AvgByTypeJoinProcessor {
	processor := &AvgByTypeJoinProcessor{
		stores: make(map[string]*AvgByTypeResultStore),
	}
	processor.tracker = NewAvgByTypeJoinCheckpointTracker(processor.getOrCreateStore)
	return processor
}

func (p *AvgByTypeJoinProcessor) Process(clientID string, msg *protobuf.AvgByTypeResult, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)
	p.tracker.MarkResultAdded(clientID, msg.GetAccount())

	return nil
}

func (p *AvgByTypeJoinProcessor) Finalize(clientID string, yield func(result *protobuf.AvgByTypeResult) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		p.tracker.ClearClient(clientID)
		delete(p.stores, clientID)
	}()

	totalPairs := 0

	results := store.GetResults()

	for _, result := range results {

		totalPairs++

		if err := yield(result); err != nil {
			return 0, err
		}
	}

	return uint64(totalPairs), nil
}

func (p *AvgByTypeJoinProcessor) Cleanup(clientID string) error {
	store := p.getOrCreateStore(clientID)
	store.Clear()
	p.tracker.ClearClient(clientID)
	delete(p.stores, clientID)
	return nil
}

func (p *AvgByTypeJoinProcessor) getOrCreateStore(clientID string) *AvgByTypeResultStore {
	store, exists := p.stores[clientID]
	if !exists {
		store = newAvgByTypeResultStore()
		p.stores[clientID] = store
	}
	return store
}

func (p *AvgByTypeJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *AvgByTypeJoinProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *AvgByTypeJoinProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *AvgByTypeJoinProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
