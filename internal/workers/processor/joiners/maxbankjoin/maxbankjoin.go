package maxbankjoin

import (
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MaxBankJoinProcessor struct {
	stores   map[string]*MaxBankStore
	storesMu sync.RWMutex
}

func NewMaxBankJoinProcessor() *MaxBankJoinProcessor {
	return &MaxBankJoinProcessor{
		stores: make(map[string]*MaxBankStore),
	}
}

func (p *MaxBankJoinProcessor) Process(clientID string, msg *protobuf.MaxBankResult) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)

	return nil
}

func (p *MaxBankJoinProcessor) Finalize(clientID string, yield func(result *protobuf.MaxBankResult) error) (uint64, error) {
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

func (p *MaxBankJoinProcessor) Cleanup(clientID string) error {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	return nil
}

func (p *MaxBankJoinProcessor) getOrCreateStore(clientID string) *MaxBankStore {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store, exists := p.stores[clientID]
	if !exists {
		store = newMaxBankStore()
		p.stores[clientID] = store
	}
	return store
}
