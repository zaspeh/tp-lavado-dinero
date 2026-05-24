package aggregatebyintermediary

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"

type IntermediaryRelations struct {
	Origins      map[model.Account]struct{}
	Destinations map[model.Account]struct{}
}
