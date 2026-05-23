package batch

import (
	"fmt"
)

type Batcher[T any, V any] struct {
	batch   *Batch[T, V]
	onFlush func(V) error
}

func NewBatcher[T any, V any](b *Batch[T, V], onFlush func(V) error) *Batcher[T, V] {
	return &Batcher[T, V]{batch: b, onFlush: onFlush}
}

func (s *Batcher[T, V]) Add(item T) error {
	if !s.batch.TryAdd(item) {
		if err := s.onFlush(s.batch.Flush()); err != nil {
			return err
		}
		if !s.batch.TryAdd(item) {
			return fmt.Errorf("item exceeds max batch size")
		}
	}
	return nil
}

func (s *Batcher[T, V]) Flush() error {
	if s.batch.IsEmpty() {
		return nil
	}
	return s.onFlush(s.batch.Flush())
}
