package microtransactionjoin

import (
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MicrotransactionJoinProcessor struct {
	stores    map[string]*MicrotransactionStore
	storesMu  sync.RWMutex
	tracker   *MicrotransactionJoinCheckpointTracker
}

func NewMicrotransactionJoinProcessor() *MicrotransactionJoinProcessor {
	processor := &MicrotransactionJoinProcessor{
		stores: make(map[string]*MicrotransactionStore),
	}
	processor.tracker = NewMicrotransactionJoinCheckpointTracker(&processor.stores)
	return processor
}

func (p *MicrotransactionJoinProcessor) Process(clientID string, msg *protobuf.Microtransaction, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)
	p.tracker.MarkResultAdded(clientID)

	return nil
}

func (p *MicrotransactionJoinProcessor) Finalize(clientID string, yield func(result *protobuf.Microtransaction) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		p.storesMu.Lock()
		delete(p.stores, clientID)
		p.storesMu.Unlock()
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

func (p *MicrotransactionJoinProcessor) Cleanup(clientID string) error {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	p.tracker.ClearClient(clientID)
	return nil
}

func (p *MicrotransactionJoinProcessor) getOrCreateStore(clientID string) *MicrotransactionStore {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store, exists := p.stores[clientID]
	if !exists {
		store = newMicrotransactionStore()
		p.stores[clientID] = store
	}
	return store
}

func (p *MicrotransactionJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *MicrotransactionJoinProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *MicrotransactionJoinProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *MicrotransactionJoinProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
