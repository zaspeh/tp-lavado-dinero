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
