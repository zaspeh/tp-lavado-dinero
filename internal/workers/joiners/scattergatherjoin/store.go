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
