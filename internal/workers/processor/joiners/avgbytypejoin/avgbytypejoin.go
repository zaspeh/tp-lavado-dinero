package avgbytypejoin

import (
	"encoding/json"
	"fmt"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type avgByTypeResultEntity struct {
	Account    string `json:"account"`
	AmountPaid string `json:"amountPaid"`
}

type AvgByTypeJoinProcessor struct {
	stores map[string]*AvgByTypeResultStore
}

func NewAvgByTypeJoinProcessor() *AvgByTypeJoinProcessor {
	return &AvgByTypeJoinProcessor{
		stores: make(map[string]*AvgByTypeResultStore),
	}
}

func (p *AvgByTypeJoinProcessor) Process(clientID string, msg *protobuf.AvgByTypeResult, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	store.Add(msg)

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "results")
	}
	return nil
}

func (p *AvgByTypeJoinProcessor) Finalize(clientID string, yield func(result *protobuf.AvgByTypeResult) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
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

func (p *AvgByTypeJoinProcessor) ListEntities(clientID string) ([]string, error) {
	if _, ok := p.stores[clientID]; !ok {
		return nil, nil
	}
	return []string{"results"}, nil
}

func (p *AvgByTypeJoinProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	if entityID != "results" {
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}

	store := p.stores[clientID]
	if store == nil {
		return nil, fmt.Errorf("store not found for client: %s", clientID)
	}

	results := store.GetResults()
	entities := make([]avgByTypeResultEntity, 0, len(results))
	for _, r := range results {
		entities = append(entities, avgByTypeResultEntity{
			Account:    r.GetAccount(),
			AmountPaid: r.GetAmountPaid(),
		})
	}

	return json.Marshal(entities)
}

func (p *AvgByTypeJoinProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	if entityID != "results" {
		return fmt.Errorf("unknown entity: %s", entityID)
	}

	var entities []avgByTypeResultEntity
	if err := json.Unmarshal(data, &entities); err != nil {
		return err
	}

	store := p.getOrCreateStore(clientID)
	for _, e := range entities {
		store.Add(&protobuf.AvgByTypeResult{
			Account:    e.Account,
			AmountPaid: e.AmountPaid,
		})
	}

	return nil
}

func (p *AvgByTypeJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
