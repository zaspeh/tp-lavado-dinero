package scattergatherjoin

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type ScatterGatherJoinProcessor struct {
	stores   map[string]*ScatterGatherStore
	storesMu sync.RWMutex
}

type pairEntity struct {
	OriginBank        int32  `json:"originBank"`
	OriginAccount     string `json:"originAccount"`
	DestBank          int32  `json:"destBank"`
	DestAccount       string `json:"destAccount"`
	IntermediaryCount int    `json:"intermediaryCount"`
}

func NewScatterGatherJoinProcessor() *ScatterGatherJoinProcessor {
	return &ScatterGatherJoinProcessor{
		stores: make(map[string]*ScatterGatherStore),
	}
}

func (p *ScatterGatherJoinProcessor) Process(clientID string, path *protobuf.SuspiciousPath, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	slog.Debug("Handling SuspiciousPathBatch Origin", "origin bank", path.GetOrigin().GetBank(), "origin account", path.GetOrigin().GetAccount(), "Destination", path.GetDestination().GetBank(), "destination account", path.GetDestination().GetAccount(), "count", path.GetIntermediaryCount())

	pair := model.OriginDestinationPair{
		Origin: model.Account{
			Bank:    path.GetOrigin().GetBank(),
			Account: path.GetOrigin().GetAccount(),
		},
		Destination: model.Account{
			Bank:    path.GetDestination().GetBank(),
			Account: path.GetDestination().GetAccount(),
		},
	}

	store.Add(pair, int(path.GetIntermediaryCount()))

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "paths")
	}

	return nil
}

func (p *ScatterGatherJoinProcessor) Finalize(clientID string, yield func(result *protobuf.Account) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		p.storesMu.Lock()
		delete(p.stores, clientID)
		p.storesMu.Unlock()
	}()

	totalPairs := 0

	suspiciousAccounts := make(map[model.Account]struct{})

	paths := store.GetPaths()

	for pair, count := range paths {
		if count < 5 {
			continue
		}

		origin := pair.Origin

		totalPairs++

		suspiciousAccounts[origin] = struct{}{}
	}

	for account := range suspiciousAccounts {
		slog.Debug(
			"adding suspicious account to response",
			"account", account.GetAccount(),
			"bank", account.GetBank(),
		)

		protoAccount := &protobuf.Account{
			Bank:    account.GetBank(),
			Account: account.GetAccount(),
		}

		if err := yield(protoAccount); err != nil {
			return 0, err
		}
	}

	return uint64(totalPairs), nil
}

func (p *ScatterGatherJoinProcessor) Cleanup(clientID string) error {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	return nil
}

func (p *ScatterGatherJoinProcessor) getOrCreateStore(clientID string) *ScatterGatherStore {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store, exists := p.stores[clientID]
	if !exists {
		store = NewScatterGatherStore()
		p.stores[clientID] = store
	}
	return store
}

func (p *ScatterGatherJoinProcessor) ListEntities(clientID string) ([]string, error) {
	p.storesMu.RLock()
	defer p.storesMu.RUnlock()

	if _, ok := p.stores[clientID]; !ok {
		return nil, nil
	}
	return []string{"paths"}, nil
}

func (p *ScatterGatherJoinProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	if entityID != "paths" {
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}

	p.storesMu.RLock()
	defer p.storesMu.RUnlock()

	store := p.stores[clientID]
	if store == nil {
		return nil, fmt.Errorf("store not found for client: %s", clientID)
	}

	paths := store.GetPaths()
	entities := make([]pairEntity, 0, len(paths))
	for pair, count := range paths {
		entities = append(entities, pairEntity{
			OriginBank:        pair.Origin.Bank,
			OriginAccount:     pair.Origin.Account,
			DestBank:          pair.Destination.Bank,
			DestAccount:       pair.Destination.Account,
			IntermediaryCount: count,
		})
	}

	return json.Marshal(entities)
}

func (p *ScatterGatherJoinProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	if entityID != "paths" {
		return fmt.Errorf("unknown entity: %s", entityID)
	}

	var entities []pairEntity
	if err := json.Unmarshal(data, &entities); err != nil {
		return err
	}

	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	for _, e := range entities {
		store.SetPairCount(model.OriginDestinationPair{
			Origin: model.Account{
				Bank:    e.OriginBank,
				Account: e.OriginAccount,
			},
			Destination: model.Account{
				Bank:    e.DestBank,
				Account: e.DestAccount,
			},
		}, e.IntermediaryCount)
	}

	return nil
}

func (p *ScatterGatherJoinProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
