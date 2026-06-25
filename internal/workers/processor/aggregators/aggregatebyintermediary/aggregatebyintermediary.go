package aggregatebyintermediary

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type AggregateByIntermediaryProcessor struct {
	stores  map[string]*IntermediaryStore
	tracker *AggregateByIntermediaryCheckpointTracker
}

func NewAggregateByIntermediaryProcessor() *AggregateByIntermediaryProcessor {
	processor := &AggregateByIntermediaryProcessor{
		stores: make(map[string]*IntermediaryStore),
	}
	processor.tracker = NewAggregateByIntermediaryCheckpointTracker(processor.getOrCreateStore)
	return processor
}

func (p *AggregateByIntermediaryProcessor) Process(clientID string, msg IntermediaryPairEvent, cm *checkpoint.CheckpointManager) error {
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

		if store.AddOrigin(intermediary, origin) {
			p.tracker.MarkOriginAdded(clientID, intermediary, origin)
		}
	} else {
		destination := model.Account{
			Bank:    msg.Pair.GetAccount().GetBank(),
			Account: msg.Pair.GetAccount().GetAccount(),
		}

		if store.AddDestination(intermediary, destination) {
			p.tracker.MarkDestinationAdded(clientID, intermediary, destination)
		}
	}

	slog.Debug("Origin intermediary pair: ", "Account", msg.Pair.GetAccount().GetAccount(), "Intermediary", msg.Pair.GetIntermediary().GetAccount())

	return nil
}

func (p *AggregateByIntermediaryProcessor) getOrCreateStore(clientID string) *IntermediaryStore {
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
		p.tracker.ClearClient(clientID)
		delete(p.stores, clientID)
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
	store := p.getOrCreateStore(clientID)
	store.Clear()
	delete(p.stores, clientID)
	p.tracker.ClearClient(clientID)
	return nil
}

func (p *AggregateByIntermediaryProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *AggregateByIntermediaryProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *AggregateByIntermediaryProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *AggregateByIntermediaryProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
