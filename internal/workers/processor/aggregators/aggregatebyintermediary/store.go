package aggregatebyintermediary

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
)

type IntermediaryStore struct {
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
	relations := s.getOrCreateRelations(intermediary)

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

func (s *IntermediaryStore) AddDestination(intermediary model.Account, destination model.Account) {
	relations := s.getOrCreateRelations(intermediary)

	if _, exists := relations.Destinations[destination]; exists {
		return
	}

	for origin := range relations.Origins {
		pair := model.OriginDestinationPair{
			Origin:      origin,
			Destination: destination,
		}
		s.pairs[pair]++
	}

	relations.Destinations[destination] = struct{}{}
}

func (s *IntermediaryStore) getOrCreateRelations(
	intermediary model.Account,
) *IntermediaryRelations {

	relations, ok := s.relations[intermediary]

	if !ok {
		relations = &IntermediaryRelations{
			Origins:      make(map[model.Account]struct{}),
			Destinations: make(map[model.Account]struct{}),
		}

		s.relations[intermediary] = relations
	}

	return relations
}

func (s *IntermediaryStore) GetPairs() map[model.OriginDestinationPair]int {
	result := make(map[model.OriginDestinationPair]int)

	for pair, count := range s.pairs {
		result[pair] = count
	}

	return result
}

func (s *IntermediaryStore) Clear() {
	s.relations = make(map[model.Account]*IntermediaryRelations)
	s.pairs = make(map[model.OriginDestinationPair]int)
}

// func (s *IntermediaryStore) AddPairWithCount(pair model.OriginDestinationPair, count int) {
// 	s.mu.Lock()
// 	defer s.mu.Unlock()

// 	s.pairs[pair] = count
// }
