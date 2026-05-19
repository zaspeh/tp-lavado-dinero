package maxbank

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

type Batch struct {
	maxWeight     int
	currentWeight int
	items         []*protobuf.MaxBankResult
}

func NewBatch(maxWeight int) *Batch {
	return &Batch{
		maxWeight: maxWeight,
		items:     make([]*protobuf.MaxBankResult, 0),
	}
}

func (b *Batch) IsFull(item *protobuf.MaxBankResult) bool {
	if b.currentWeight+proto.Size(item) > b.maxWeight {
		return true
	}
	return false
}

func (b *Batch) Add(item *protobuf.MaxBankResult) bool {
	if b.IsFull(item) {
		return false
	}
	addedWeight := proto.Size(item)
	b.items = append(b.items, item)
	b.currentWeight += addedWeight
	return true
}

func (b *Batch) Get() *protobuf.MaxBankResultBatch {
	protobatch := &protobuf.MaxBankResultBatch{
		Results: b.items,
	}
	b.Clear()
	return protobatch
}

func (b *Batch) isEmpty() bool {
	return len(b.items) == 0
}

func (b *Batch) Clear() {
	b.items = b.items[:0]
	b.currentWeight = 0
}
