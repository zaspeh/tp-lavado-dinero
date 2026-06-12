package batch

import (
	"fmt"

	"github.com/google/uuid"
)

type Batcher[T any, V any] struct {
	batch        *Batch[T, V]
	onFlush      func(V) error
	batchFlushed uint
}

func NewBatcher[T any, V any](b *Batch[T, V], onFlush func(V) error) *Batcher[T, V] {
	return &Batcher[T, V]{batch: b, onFlush: onFlush}
}

func (s *Batcher[T, V]) SetNewBatchId(batchId string) {
	if noBatchId == batchId {
		batchId = uuid.New().String()
	}
	s.batch.id = batchId
	s.batchFlushed = 0
}

func (s *Batcher[T, V]) Add(item T) error {
	if !s.batch.TryAdd(item) {
		if err := s.onFlush(s.batch.Flush()); err != nil {
			return err
		}
		if !s.batch.TryAdd(item) {
			return fmt.Errorf("item exceeds max batch size")
		}
		s.batchFlushed++
		newBatchId := fmt.Sprintf("%s-%d", s.batch.id, s.batchFlushed)
		s.batch.id = newBatchId
	}
	return nil
}

func (s *Batcher[T, V]) Flush() error {
	if s.batch.IsEmpty() {
		return nil
	}
	return s.onFlush(s.batch.Flush())
}
