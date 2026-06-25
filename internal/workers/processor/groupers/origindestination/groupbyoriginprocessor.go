package origindestination

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type GroupByOriginProcessor struct {
	originStores map[string]*AccountStore
	tracker      *AccountStoreCheckpointTracker
}

func NewGroupByOriginProcessor() *GroupByOriginProcessor {
	processor := &GroupByOriginProcessor{
		originStores: make(map[string]*AccountStore),
	}
	processor.tracker = NewAccountStoreCheckpointTracker(processor.getOrCreateStore)
	return processor
}

func (p *GroupByOriginProcessor) Process(clientID string, scatterGatherMsg *protobuf.ScatterGather, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	origin := Account{
		Bank:    scatterGatherMsg.GetFromBank(),
		Account: scatterGatherMsg.GetAccount(),
	}

	destination := Account{
		Bank:    scatterGatherMsg.GetToBank(),
		Account: scatterGatherMsg.GetToAccount(),
	}

	if store.Add(origin, destination) {
		p.tracker.MarkRelationAdded(clientID, origin, destination)
	}

	return nil
}

func (p *GroupByOriginProcessor) getOrCreateStore(clientID string) *AccountStore {
	store, exists := p.originStores[clientID]
	if !exists {
		store = newAccountStore()
		p.originStores[clientID] = store
	}
	return store
}

func (w *GroupByOriginProcessor) Finalize(clientID string, yield func(result *protobuf.GroupedAccounts) error) (uint64, error) {
	store := w.getOrCreateStore(clientID)
	data := store.GetData()
	totalGroups := 0

	for origin, destinationsMap := range data {
		originBank := origin.GetBank()
		originAccount := origin.GetAccount()

		if len(destinationsMap) < 5 {
			continue
		}
		totalGroups++

		slog.Debug("Origin Account: ", "ACcount", originAccount, "len", len(destinationsMap))

		group := &protobuf.GroupedAccounts{
			BaseAccount: &protobuf.Account{
				Bank:    originBank,
				Account: originAccount,
			},
		}

		for destination := range destinationsMap {
			group.RelatedAccounts = append(group.RelatedAccounts, &protobuf.Account{
				Bank:    destination.GetBank(),
				Account: destination.GetAccount(),
			})
		}

		if err := yield(group); err != nil {
			return 0, err
		}
	}

	store.Clear()
	w.tracker.ClearClient(clientID)
	delete(w.originStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByOriginProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	w.tracker.ClearClient(clientID)
	delete(w.originStores, clientID)
	return nil
}

func (w *GroupByOriginProcessor) ClearClientState(clientID string) error {
	return w.Cleanup(clientID)
}

func (w *GroupByOriginProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return w.tracker.DrainChanges(clientID)
}

func (w *GroupByOriginProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return w.tracker.RestoreChanges(clientID, changes)
}

func (w *GroupByOriginProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return w.tracker.ApplyChange(clientID, change)
}
