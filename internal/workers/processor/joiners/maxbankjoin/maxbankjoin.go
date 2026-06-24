package maxbankjoin

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MaxBankJoinProcessor struct {
	stores  map[string]*MaxBankStore
	tracker *MaxBankJoinCheckpointTracker
}

func NewMaxBankJoinProcessor() *MaxBankJoinProcessor {
	processor := &MaxBankJoinProcessor{
		stores: make(map[string]*MaxBankStore),
	}
	processor.tracker = NewMaxBankJoinCheckpointTracker(&processor.stores)
	return processor
}

func (p *MaxBankJoinProcessor) Process(clientID string, msg *protobuf.MaxBankResult, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)
	p.tracker.MarkResultAdded(clientID)

	return nil
}

func (p *MaxBankJoinProcessor) Finalize(clientID string, yield func(result *protobuf.MaxBankResult) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		delete(p.stores, clientID)
		p.tracker.ClearClient(clientID)
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

func (p *MaxBankJoinProcessor) Cleanup(clientID string) error {
	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	p.tracker.ClearClient(clientID)
	return nil
}

func (p *MaxBankJoinProcessor) getOrCreateStore(clientID string) *MaxBankStore {
	store, exists := p.stores[clientID]
	if !exists {
		store = newMaxBankStore()
		p.stores[clientID] = store
	}
	return store
}

func (p *MaxBankJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *MaxBankJoinProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *MaxBankJoinProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *MaxBankJoinProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
