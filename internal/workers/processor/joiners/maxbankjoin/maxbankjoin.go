package maxbankjoin

import (
	"encoding/json"
	"fmt"
	"sync"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MaxBankJoinProcessor struct {
	stores   map[string]*MaxBankStore
	storesMu sync.RWMutex
}

type maxBankResultEntity struct {
	BankName string `json:"bankName"`
	Account string `json:"account"`
	Amount  string `json:"amount"`
}

func NewMaxBankJoinProcessor() *MaxBankJoinProcessor {
	return &MaxBankJoinProcessor{
		stores: make(map[string]*MaxBankStore),
	}
}

func (p *MaxBankJoinProcessor) Process(clientID string, msg *protobuf.MaxBankResult, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "results")
	}

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

func (p *MaxBankJoinProcessor) ListEntities(clientID string) ([]string, error) {
	p.storesMu.RLock()
	defer p.storesMu.RUnlock()

	if _, ok := p.stores[clientID]; !ok {
		return nil, nil
	}
	return []string{"results"}, nil
}

func (p *MaxBankJoinProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
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
	entities := make([]maxBankResultEntity, 0, len(results))
	for _, r := range results {
		entities = append(entities, maxBankResultEntity{
			BankName: r.GetBankName(),
			Account: r.GetAccount(),
			Amount:  r.GetAmount(),
		})
	}

	return json.Marshal(entities)
}

func (p *MaxBankJoinProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	if entityID != "results" {
		return fmt.Errorf("unknown entity: %s", entityID)
	}

	var entities []maxBankResultEntity
	if err := json.Unmarshal(data, &entities); err != nil {
		return err
	}

	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	for _, e := range entities {
		store.Add(&protobuf.MaxBankResult{
			BankName: e.BankName,
			Account:  e.Account,
			Amount:   e.Amount,
		})
	}

	return nil
}

func (p *MaxBankJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
