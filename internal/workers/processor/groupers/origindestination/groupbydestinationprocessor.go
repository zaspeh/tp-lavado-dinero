package origindestination

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type GroupByDestinationProcessor struct {
	destinationStores map[string]*AccountStore
}

type destStoreEntity struct {
	Destinations []destStoreEntry `json:"destinations"`
}

type destStoreEntry struct {
	DestBank    int32        `json:"destBank"`
	DestAccount string       `json:"destAccount"`
	Origins     []origEntity `json:"origins"`
}

type origEntity struct {
	OrigBank    int32  `json:"origBank"`
	OrigAccount string `json:"origAccount"`
}

func NewGroupByDestinationProcessor() *GroupByDestinationProcessor {
	return &GroupByDestinationProcessor{
		destinationStores: make(map[string]*AccountStore),
	}
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

	store.Add(destination, origin)

	//if cm != nil {
	//	cm.NotifyEntityChanged(clientID, "destinations")
	//}

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
	delete(w.destinationStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByDestinationProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	delete(w.destinationStores, clientID)
	return nil
}

func (w *GroupByDestinationProcessor) ListEntities(clientID string) ([]string, error) {
	// if _, ok := w.destinationStores[clientID]; !ok {
	// 	return nil, nil
	// }
	return []string{"destinations"}, nil
}

func (w *GroupByDestinationProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	// if entityID != "destinations" {
	// 	return nil, fmt.Errorf("unknown entity: %s", entityID)
	// }

	// store := w.destinationStores[clientID]
	// if store == nil {
	// 	return nil, fmt.Errorf("store not found for client: %s", clientID)
	// }

	// data := store.GetData()
	// entries := make([]destStoreEntry, 0)
	// for dest, originsMap := range data {
	// 	origins := make([]origEntity, 0, len(originsMap))
	// 	for orig := range originsMap {
	// 		origins = append(origins, origEntity{
	// 			OrigBank:    orig.Bank,
	// 			OrigAccount: orig.Account,
	// 		})
	// 	}
	// 	entries = append(entries, destStoreEntry{
	// 		DestBank:    dest.Bank,
	// 		DestAccount: dest.Account,
	// 		Origins:     origins,
	// 	})
	// }

	// return json.Marshal(entries)
	return []byte{}, nil
}

func (w *GroupByDestinationProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	// if entityID != "destinations" {
	// 	return fmt.Errorf("unknown entity: %s", entityID)
	// }

	// var entries []destStoreEntry
	// if err := json.Unmarshal(data, &entries); err != nil {
	// 	return err
	// }

	// store := w.getOrCreateStore(clientID)
	// for _, e := range entries {
	// 	for _, o := range e.Origins {
	// 		store.Add(Account{Bank: e.DestBank, Account: e.DestAccount}, Account{Bank: o.OrigBank, Account: o.OrigAccount})
	// 	}
	// }

	return nil
}

func (w *GroupByDestinationProcessor) ClearClientState(clientID string) error {
	//return w.Cleanup(clientID)
	return nil
}
