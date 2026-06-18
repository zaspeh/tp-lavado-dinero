package batch

import (
	"fmt"

	"github.com/google/uuid"
)

type Batcher[T any, V any] struct {
	batch        *Batch[T, V]
	onFlush      func(V, string) error
	batchFlushed uint
}

func NewBatcher[T any, V any](b *Batch[T, V], onFlush func(V, string) error) *Batcher[T, V] {
	return &Batcher[T, V]{batch: b, onFlush: onFlush}
}

func (s *Batcher[T, V]) SetNewBatchId(batchID string) {
	if DefaultBatchId == batchID {
		batchID = uuid.New().String()
	}

	// si es el mismo que ya tenia, no hago nada
	if batchID == s.batch.baseID {
		return
	}

	s.batch.baseID = batchID
	s.batch.subID = fmt.Sprintf("%s-0", batchID)
	s.batchFlushed = 0
}

func (s *Batcher[T, V]) Add(item T) error {
	if !s.batch.TryAdd(item) {
		if err := s.onFlush(s.batch.Flush(), s.batch.subID); err != nil {
			return err
		}
		if !s.batch.TryAdd(item) {
			return fmt.Errorf("item exceeds max batch size")
		}
		s.batchFlushed++
		newBatchId := fmt.Sprintf("%s-%d", s.batch.baseID, s.batchFlushed)
		s.batch.subID = newBatchId
	}
	return nil
}

func (s *Batcher[T, V]) Flush() error {
	if s.batch.IsEmpty() {
		return nil
	}
	return s.onFlush(s.batch.Flush(), s.batch.subID)
}
