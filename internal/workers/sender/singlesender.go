package sender

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type SingleSender[T any, V any] struct {
	output     m.Middleware
	wrapper    batch.Wrapper[T, V]
	sizer      batch.Sizer[T]
	batchers   map[string]*batch.Batcher[T, V]
	serializer func(clientID string, batch V) (m.Message, error)
	maxWeight  int
}

func NewSingleSender[T any, V any](output m.Middleware, wrapper batch.Wrapper[T, V],
	sizer batch.Sizer[T], maxWeight int, serializer func(clientID string, batch V) (m.Message, error),
) *SingleSender[T, V] {
	return &SingleSender[T, V]{
		output:     output,
		wrapper:    wrapper,
		sizer:      sizer,
		maxWeight:  maxWeight,
		serializer: serializer,
		batchers:   make(map[string]*batch.Batcher[T, V]),
	}
}

func (s *SingleSender[T, V]) Add(clientID string, item T) error {
	batcher, exists := s.batchers[clientID]
	if !exists {
		newBatch := batch.New(s.maxWeight, s.sizer, s.wrapper)

		onFlush := func(batch V) error {
			return s.flushBatch(clientID, batch)
		}

		batcher = batch.NewBatcher(newBatch, onFlush)
		s.batchers[clientID] = batcher
	}
	return batcher.Add(item)
}

func (s *SingleSender[T, V]) Flush(clientID string) error {
	if batcher, exists := s.batchers[clientID]; exists {
		return batcher.Flush()
	}
	return nil
}

func (s *SingleSender[T, V]) Cleanup(clientID string) error {
	delete(s.batchers, clientID)
	return nil
}

func (s *SingleSender[T, V]) Close() error {
	for clientID := range s.batchers {
		s.Cleanup(clientID)
	}
	return s.output.Close()
}

func (s *SingleSender[T, V]) flushBatch(clientID string, batch V) error {
	msg, err := s.serializer(clientID, batch)
	if err != nil {
		return err
	}
	return s.output.Send(msg)
}
