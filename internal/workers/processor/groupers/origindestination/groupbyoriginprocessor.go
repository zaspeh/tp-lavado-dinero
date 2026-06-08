package origindestination

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type GroupByOriginProcessor struct {
	originStores map[string]*AccountStore
}

func NewGroupByOriginProcessor() *GroupByOriginProcessor {
	return &GroupByOriginProcessor{
		originStores: make(map[string]*AccountStore),
	}
}

func (p *GroupByOriginProcessor) Process(clientID string, scatterGatherMsg *protobuf.ScatterGather) error {
	store := p.getOrCreateStore(clientID)

	origin := Account{
		Bank:    scatterGatherMsg.GetFromBank(),
		Account: scatterGatherMsg.GetAccount(),
	}

	destination := Account{
		Bank:    scatterGatherMsg.GetToBank(),
		Account: scatterGatherMsg.GetToAccount(),
	}

	store.Add(origin, destination)

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
	delete(w.originStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByOriginProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	delete(w.originStores, clientID)
	return nil
}
