package aggregatebyintermediary

import (
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type AggregateByIntermediaryProcessor struct {
	stores   map[string]*IntermediaryStore
	storesMu sync.RWMutex
}

func NewAggregateByIntermediaryProcessor() *AggregateByIntermediaryProcessor {
	return &AggregateByIntermediaryProcessor{
		stores: make(map[string]*IntermediaryStore),
	}
}

func (p *AggregateByIntermediaryProcessor) Process(clientID string, msg IntermediaryPairEvent) error {
	store := p.getOrCreateStore(clientID)

	intermediary := model.Account{
		Bank:    msg.Pair.GetIntermediary().GetBank(),
		Account: msg.Pair.GetIntermediary().GetAccount(),
	}

	if msg.IsOrigin {
		origin := model.Account{
			Bank:    msg.Pair.GetAccount().GetBank(),
			Account: msg.Pair.GetAccount().GetAccount(),
		}

		store.AddOrigin(intermediary, origin)
	} else {
		destination := model.Account{
			Bank:    msg.Pair.GetAccount().GetBank(),
			Account: msg.Pair.GetAccount().GetAccount(),
		}

		store.AddDestination(intermediary, destination)
	}

	return nil
}

func (p *AggregateByIntermediaryProcessor) getOrCreateStore(clientID string) *IntermediaryStore {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store, exists := p.stores[clientID]
	if !exists {
		store = NewIntermediaryStore()
		p.stores[clientID] = store
	}
	return store
}

func (p *AggregateByIntermediaryProcessor) Finalize(clientID string, yield func(result *protobuf.SuspiciousPath) error) (uint64, error) {
	store := p.getOrCreateStore(clientID)
	defer func() {
		store.Clear()
		p.storesMu.Lock()
		delete(p.stores, clientID)
		p.storesMu.Unlock()
	}()

	totalPairs := 0

	pairs := store.GetPairs()

	for pair, intermediaryCount := range pairs {
		path := &protobuf.SuspiciousPath{
			Origin: &protobuf.Account{
				Bank:    pair.Origin.Bank,
				Account: pair.Origin.Account,
			},

			Destination: &protobuf.Account{
				Bank:    pair.Destination.Bank,
				Account: pair.Destination.Account,
			},

			IntermediaryCount: uint32(intermediaryCount),
		}
		totalPairs++

		if err := yield(path); err != nil {
			return 0, err
		}
	}

	return uint64(totalPairs), nil
}

func (p *AggregateByIntermediaryProcessor) Cleanup(clientID string) error {
	p.storesMu.Lock()
	defer p.storesMu.Unlock()

	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	return nil
}
