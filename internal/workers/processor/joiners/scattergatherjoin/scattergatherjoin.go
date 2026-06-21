package scattergatherjoin

import (
	"log/slog"
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type ScatterGatherJoinProcessor struct {
	stores   map[string]*ScatterGatherStore
	storesMu sync.RWMutex
}

func NewScatterGatherJoinProcessor() *ScatterGatherJoinProcessor {
	return &ScatterGatherJoinProcessor{
		stores: make(map[string]*ScatterGatherStore),
	}
}

func (p *ScatterGatherJoinProcessor) Process(clientID string, path *protobuf.SuspiciousPath) error {
	store := p.getOrCreateStore(clientID)

	slog.Debug("Handling SuspiciousPathBatch Origin", "origin bank", path.GetOrigin().GetBank(), "origin account", path.GetOrigin().GetAccount(), "Destination", path.GetDestination().GetBank(), "destination account", path.GetDestination().GetAccount())

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
