package origindestination

import (
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type GroupByOriginProcessor struct {
	originStores map[string]*AccountStore
}

type originStoreEntity struct {
	Origins []originEntity `json:"origins"`
}

type originEntity struct {
	OriginBank    int32        `json:"originBank"`
	OriginAccount string       `json:"originAccount"`
	Destinations  []destEntity `json:"destinations"`
}

type destEntity struct {
	DestBank    int32  `json:"destBank"`
	DestAccount string `json:"destAccount"`
}

func NewGroupByOriginProcessor() *GroupByOriginProcessor {
	return &GroupByOriginProcessor{
		originStores: make(map[string]*AccountStore),
	}
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

	store.Add(origin, destination)

	//if cm != nil {
	//	cm.NotifyEntityChanged(clientID, "origins")
	//}

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
	delete(w.originStores, clientID)
	return uint64(totalGroups), nil
}

func (w *GroupByOriginProcessor) Cleanup(clientID string) error {
	store := w.getOrCreateStore(clientID)
	store.Clear()
	delete(w.originStores, clientID)
	return nil
}

func (w *GroupByOriginProcessor) ListEntities(clientID string) ([]string, error) {
	// if _, ok := w.originStores[clientID]; !ok {
	// 	return nil, nil
	// }
	return []string{"origins"}, nil
}

func (w *GroupByOriginProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	// if entityID != "origins" {
	// 	return nil, fmt.Errorf("unknown entity: %s", entityID)
	// }

	// store := w.originStores[clientID]
	// if store == nil {
	// 	return nil, fmt.Errorf("store not found for client: %s", clientID)
	// }

	// data := store.GetData()
	// entities := make([]originEntity, 0)
	// for origin, destinationsMap := range data {
	// 	dests := make([]destEntity, 0, len(destinationsMap))
	// 	for dest := range destinationsMap {
	// 		dests = append(dests, destEntity{
	// 			DestBank:    dest.Bank,
	// 			DestAccount: dest.Account,
	// 		})
	// 	}
	// 	entities = append(entities, originEntity{
	// 		OriginBank:    origin.Bank,
	// 		OriginAccount: origin.Account,
	// 		Destinations:  dests,
	// 	})
	// }

	// return json.Marshal(entities)
	return []byte{}, nil
}

func (w *GroupByOriginProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	// if entityID != "origins" {
	// 	return fmt.Errorf("unknown entity: %s", entityID)
	// }

	// var entities []originEntity
	// if err := json.Unmarshal(data, &entities); err != nil {
	// 	return err
	// }

	// store := w.getOrCreateStore(clientID)
	// for _, e := range entities {
	// 	for _, d := range e.Destinations {
	// 		store.Add(Account{Bank: e.OriginBank, Account: e.OriginAccount}, Account{Bank: d.DestBank, Account: d.DestAccount})
	// 	}
	// }

	return nil
}

func (w *GroupByOriginProcessor) ClearClientState(clientID string) error {
	//return w.Cleanup(clientID)
	return nil
}
