package aggregatebyintermediary

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type storeEntity struct {
	Relations map[string][]string `json:"relations"`
	Pairs     []pairEntity        `json:"pairs"`
}

type pairEntity struct {
	OriginBank          int32  `json:"originBank"`
	OriginAccount       string `json:"originAccount"`
	DestBank            int32  `json:"destBank"`
	DestAccount         string `json:"destAccount"`
	IntermediaryBank    int32  `json:"intermediaryBank"`
	IntermediaryAccount string `json:"intermediaryAccount"`
	Count               int    `json:"count"`
}

type AggregateByIntermediaryProcessor struct {
	stores map[string]*IntermediaryStore
}

func NewAggregateByIntermediaryProcessor() *AggregateByIntermediaryProcessor {
	return &AggregateByIntermediaryProcessor{
		stores: make(map[string]*IntermediaryStore),
	}
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

		store.AddOrigin(intermediary, origin)
	} else {
		destination := model.Account{
			Bank:    msg.Pair.GetAccount().GetBank(),
			Account: msg.Pair.GetAccount().GetAccount(),
		}

		store.AddDestination(intermediary, destination)
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
	return nil
}

func (p *AggregateByIntermediaryProcessor) ListEntities(clientID string) ([]string, error) {
	// p.storesMu.RLock()
	// defer p.storesMu.RUnlock()

	// if _, ok := p.stores[clientID]; !ok {
	// 	return nil, nil
	// }
	return []string{"store"}, nil
}

func (p *AggregateByIntermediaryProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	// if entityID != "store" {
	// 	return nil, fmt.Errorf("unknown entity: %s", entityID)
	// }

	// p.storesMu.RLock()
	// defer p.storesMu.RUnlock()

	// store := p.stores[clientID]
	// if store == nil {
	// 	return nil, fmt.Errorf("store not found for client: %s", clientID)
	// }

	// entities := storeEntity{
	// 	Relations: make(map[string][]string),
	// 	Pairs:    make([]pairEntity, 0),
	// }

	// for intermediary, relations := range store.relations {
	// 	key := fmt.Sprintf("%d:%s", intermediary.Bank, intermediary.Account)
	// 	for origin := range relations.Origins {
	// 		entities.Relations[key] = append(entities.Relations[key], fmt.Sprintf("O:%d:%s", origin.Bank, origin.Account))
	// 	}
	// 	for dest := range relations.Destinations {
	// 		entities.Relations[key] = append(entities.Relations[key], fmt.Sprintf("D:%d:%s", dest.Bank, dest.Account))
	// 	}
	// }

	// for pair, count := range store.pairs {
	// 	entities.Pairs = append(entities.Pairs, pairEntity{
	// 		OriginBank:      pair.Origin.Bank,
	// 		OriginAccount:   pair.Origin.Account,
	// 		DestBank:        pair.Destination.Bank,
	// 		DestAccount:     pair.Destination.Account,
	// 		Count:           count,
	// 	})
	// }

	// return json.Marshal(entities)
	return []byte{}, nil
}

func (p *AggregateByIntermediaryProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	// if entityID != "store" {
	// 	return fmt.Errorf("unknown entity: %s", entityID)
	// }

	// var entities storeEntity
	// if err := json.Unmarshal(data, &entities); err != nil {
	// 	return err
	// }

	// p.storesMu.Lock()
	// defer p.storesMu.Unlock()

	// store := p.getOrCreateStore(clientID)

	// for key, values := range entities.Relations {
	// 	var iBank int32
	// 	var iAccount string
	// 	parts := splitFirst(key, ":")
	// 	fmt.Sscanf(parts[0], "%d", &iBank)
	// 	iAccount = parts[1]
	// 	intermediary := model.Account{Bank: iBank, Account: iAccount}

	// 	for _, v := range values {
	// 		var aBank int32
	// 		var aAccount string
	// 		if v[0] == 'O' {
	// 			rest := v[2:]
	// 			parts := splitFirst(rest, ":")
	// 			fmt.Sscanf(parts[0], "%d", &aBank)
	// 			aAccount = parts[1]
	// 			store.AddOrigin(intermediary, model.Account{Bank: aBank, Account: aAccount})
	// 		} else if v[0] == 'D' {
	// 			rest := v[2:]
	// 			parts := splitFirst(rest, ":")
	// 			fmt.Sscanf(parts[0], "%d", &aBank)
	// 			aAccount = parts[1]
	// 			store.AddDestination(intermediary, model.Account{Bank: aBank, Account: aAccount})
	// 		}
	// 	}
	// }

	// for _, pair := range entities.Pairs {
	// 	store.AddPairWithCount(model.OriginDestinationPair{
	// 		Origin:      model.Account{Bank: pair.OriginBank, Account: pair.OriginAccount},
	// 		Destination: model.Account{Bank: pair.DestBank, Account: pair.DestAccount},
	// 	}, pair.Count)
	// }

	return nil
}

// func splitFirst(s, sep string) [2]string {
// 	for i := 0; i < len(s); i++ {
// 		if s[i] == sep[0] {
// 			return [2]string{s[:i], s[i+1:]}
// 		}
// 	}
// 	return [2]string{s, ""}
// }

func (p *AggregateByIntermediaryProcessor) ClearClientState(clientID string) error {
	//return p.Cleanup(clientID)
	return nil
}
