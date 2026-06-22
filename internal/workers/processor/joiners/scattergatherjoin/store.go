package scattergatherjoin

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
)

type ScatterGatherStore struct {
	paths map[model.OriginDestinationPair]int
}

func NewScatterGatherStore() *ScatterGatherStore {
	return &ScatterGatherStore{
		paths: make(map[model.OriginDestinationPair]int),
	}
}

func (s *ScatterGatherStore) Add(pair model.OriginDestinationPair, count int) {
	s.paths[pair] += count
}

func (s *ScatterGatherStore) GetPaths() map[model.OriginDestinationPair]int {

	result := make(map[model.OriginDestinationPair]int)

	for pair, count := range s.paths {
		result[pair] = count
	}

	return result
}

func (s *ScatterGatherStore) Clear() {
	s.paths = make(map[model.OriginDestinationPair]int)
}

// func (s *ScatterGatherStore) SetPairCount(pair model.OriginDestinationPair, count int) {
// 	s.paths[pair] = count
// }
