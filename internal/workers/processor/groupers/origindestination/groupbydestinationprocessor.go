package origindestination

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type GroupByDestinationProcessor struct {
	destinationStores map[string]*AccountStore
	tracker           *AccountStoreCheckpointTracker
}

func NewGroupByDestinationProcessor() *GroupByDestinationProcessor {
	processor := &GroupByDestinationProcessor{
		destinationStores: make(map[string]*AccountStore),
	}
	processor.tracker = NewAccountStoreCheckpointTracker(processor.getOrCreateStore)
	return processor
}

func (p *GroupByDestinationProcessor) Process(clientID string, scatterGatherMsg *protobuf.ScatterGather, cm *checkpoint.CheckpointManager) error {
	store := p.getOrCreateStore(clientID)

	origin := Account{
		Bank:    scatterGatherMsg.GetFromBank(),
		Account: scatterGatherMsg.GetAccount(),
	}

	destination := Account{
		Bank:    scatterGatherMsg.GetToBank(),
		Account: scatterGatherMsg.GetToAccount(),
	}

	if store.Add(destination, origin) {
		p.tracker.MarkRelationAdded(clientID, destination, origin)
	}

	return nil
}

func (p *GroupByDestinationProcessor) getOrCreateStore(clientID string) *AccountStore {
	store, exists := p.destinationStores[clientID]
	if !exists {
		store = newAccountStore()
		p.destinationStores[clientID] = store
	}
	return store
}

func (w *GroupByDestinationProcessor) Finalize(clientID string, yield func(result *protobuf.GroupedAccounts) error) (uint64, error) {
	store := w.getOrCreateStore(clientID)
	data := store.GetData()
	totalGroups := 0

	for destination, originsMap := range data {
		destinationBank := destination.GetBank()
		destinationAccount := destination.GetAccount()

		if len(originsMap) < 5 {
			continue
		}
		totalGroups++

		slog.Debug("Destination Account: ", "ACcount", destinationAccount, "len", len(originsMap))

		group := &protobuf.GroupedAccounts{
			BaseAccount: &protobuf.Account{
				Bank:    destinationBank,
				Account: destinationAccount,
			},
		}

		for origin := range originsMap {
			group.RelatedAccounts = append(group.RelatedAccounts, &protobuf.Account{
				Bank:    origin.GetBank(),
				Account: origin.GetAccount(),
			})
		}

		if err := yield(group); err != nil {
			return 0, err
		}
	}

	store.Clear()
	w.tracker.ClearClient(clientID)
	delete(w.destinationStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByDestinationProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	w.tracker.ClearClient(clientID)
	delete(w.destinationStores, clientID)
	return nil
}

func (w *GroupByDestinationProcessor) ClearClientState(clientID string) error {
	return w.Cleanup(clientID)
}

func (w *GroupByDestinationProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return w.tracker.DrainChanges(clientID)
}

func (w *GroupByDestinationProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return w.tracker.RestoreChanges(clientID, changes)
}

func (w *GroupByDestinationProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return w.tracker.ApplyChange(clientID, change)
}
