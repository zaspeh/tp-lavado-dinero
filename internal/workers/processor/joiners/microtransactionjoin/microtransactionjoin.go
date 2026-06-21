package microtransactionjoin

import (
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MicrotransactionJoinProcessor struct {
	stores   map[string]*MicrotransactionStore
	storesMu sync.RWMutex
}

func NewMicrotransactionJoinProcessor() *MicrotransactionJoinProcessor {
	return &MicrotransactionJoinProcessor{
		stores: make(map[string]*MicrotransactionStore),
	}
}

func (p *MicrotransactionJoinProcessor) Process(clientID string, msg *protobuf.Microtransaction) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)

	return nil
}

func (p *MicrotransactionJoinProcessor) Finalize(clientID string, yield func(result *protobuf.Microtransaction) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		p.storesMu.Lock()
		delete(p.stores, clientID)
		p.storesMu.Unlock()
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
