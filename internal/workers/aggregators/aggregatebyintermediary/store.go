package aggregatebyintermediary

import (
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
)

type IntermediaryStore struct {
	mu sync.Mutex

	relations map[model.Account]*IntermediaryRelations
	pairs     map[model.OriginDestinationPair]int
}

func NewIntermediaryStore() *IntermediaryStore {
	return &IntermediaryStore{
		relations: make(map[model.Account]*IntermediaryRelations),
		pairs:     make(map[model.OriginDestinationPair]int),
	}
}

func (s *IntermediaryStore) AddOrigin(intermediary model.Account, origin model.Account) {

	s.mu.Lock()
	defer s.mu.Unlock()

	relations, ok := s.relations[intermediary]

	if !ok {
		relations = &IntermediaryRelations{
			Origins:      make(map[model.Account]struct{}),
			Destinations: make(map[model.Account]struct{}),
		}
		s.relations[intermediary] = relations
	}

	if _, exists := relations.Origins[origin]; exists {
		return
	}

	for destination := range relations.Destinations {
		pair := model.OriginDestinationPair{
			Origin:      origin,
			Destination: destination,
		}
		s.pairs[pair]++
	}

	relations.Origins[origin] = struct{}{}
}
