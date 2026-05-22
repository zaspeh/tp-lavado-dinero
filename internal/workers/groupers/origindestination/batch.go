package origindestination

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

type Batch struct {
	maxWeight     int
	currentWeight int
	items         []*protobuf.GroupedAccounts
}

func NewBatch(maxWeight int) *Batch {
	return &Batch{
		maxWeight: maxWeight,
		items:     make([]*protobuf.GroupedAccounts, 0),
	}
}

func (b *Batch) IsFull(
	item *protobuf.GroupedAccounts,
) bool {

	return b.currentWeight+proto.Size(item) > b.maxWeight
}

func (b *Batch) Add(
	item *protobuf.GroupedAccounts,
) bool {

	if b.IsFull(item) {
		return false
	}

	b.items = append(b.items, item)
	b.currentWeight += proto.Size(item)

	return true
}

func (b *Batch) Get() *protobuf.GroupedAccountsBatch {

	protoBatch := &protobuf.GroupedAccountsBatch{
		Groups: b.items,
	}

	b.Clear()

	return protoBatch
}

func (b *Batch) IsEmpty() bool {
	return len(b.items) == 0
}

func (b *Batch) Clear() {
	b.items = b.items[:0]
	b.currentWeight = 0
}
