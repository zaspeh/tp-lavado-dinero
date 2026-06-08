package origindestination

import (
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type GroupByDestinationProcessor struct {
	destinationStores map[string]*AccountStore
}

func NewGroupByDestinationProcessor() *GroupByDestinationProcessor {
	return &GroupByDestinationProcessor{
		destinationStores: make(map[string]*AccountStore),
	}
}

func (p *GroupByDestinationProcessor) Process(clientID string, scatterGatherMsg *protobuf.ScatterGather) error {
	store := p.getOrCreateStore(clientID)

	origin := Account{
		Bank:    scatterGatherMsg.GetFromBank(),
		Account: scatterGatherMsg.GetAccount(),
	}

	destination := Account{
		Bank:    scatterGatherMsg.GetToBank(),
		Account: scatterGatherMsg.GetToAccount(),
	}

	store.Add(destination, origin)

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
	delete(w.destinationStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByDestinationProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	delete(w.destinationStores, clientID)
	return nil
}
