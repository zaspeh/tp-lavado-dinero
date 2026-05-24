package aggregatebyintermediary

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"

type IntermediaryStore struct {
	relations map[model.Account]*IntermediaryRelations
	pairs     map[model.OriginDestinationPair]int
}
