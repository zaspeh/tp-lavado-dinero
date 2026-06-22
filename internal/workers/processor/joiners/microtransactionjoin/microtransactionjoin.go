package microtransactionjoin

import (
	"encoding/json"
	"fmt"
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MicrotransactionJoinProcessor struct {
	stores   map[string]*MicrotransactionStore
	storesMu sync.RWMutex
}

type microtransactionEntity struct {
	Account   string  `json:"account"`
	ToAccount string  `json:"toAccount"`
	Amount    float64 `json:"amount"`
}

func NewMicrotransactionJoinProcessor() *MicrotransactionJoinProcessor {
	return &MicrotransactionJoinProcessor{
		stores: make(map[string]*MicrotransactionStore),
	}
}

func (p *MicrotransactionJoinProcessor) Process(clientID string, msg *protobuf.Microtransaction, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "results")
	}

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

func (p *MicrotransactionJoinProcessor) ListEntities(clientID string) ([]string, error) {
	p.storesMu.RLock()
	defer p.storesMu.RUnlock()

	if _, ok := p.stores[clientID]; !ok {
		return nil, nil
	}
	return []string{"results"}, nil
}

func (p *MicrotransactionJoinProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	if entityID != "results" {
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}

	p.storesMu.RLock()
	defer p.storesMu.RUnlock()

	store := p.stores[clientID]
	if store == nil {
		return nil, fmt.Errorf("store not found for client: %s", clientID)
	}

	results := store.GetResults()
	entities := make([]microtransactionEntity, 0, len(results))
	for _, r := range results {
		entities = append(entities, microtransactionEntity{
			Account:   r.GetAccount(),
			ToAccount: r.GetToAccount(),
			Amount:    r.GetAmount(),
		})
	}

	return json.Marshal(entities)
}

func (p *MicrotransactionJoinProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	if entityID != "results" {
		return fmt.Errorf("unknown entity: %s", entityID)
	}

	var entities []microtransactionEntity
	if err := json.Unmarshal(data, &entities); err != nil {
		return err
	}

	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	for _, e := range entities {
		store.Add(&protobuf.Microtransaction{
			Account:   e.Account,
			ToAccount: e.ToAccount,
			Amount:    e.Amount,
		})
	}

	return nil
}

func (p *MicrotransactionJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
