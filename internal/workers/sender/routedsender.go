package sender

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

type RoutedItem[T any] struct {
	Route string
	Item  T
}

type RoutedSender[T any, B any] struct {
	exchange   *m.ExchangeMiddleware
	wrapper    batch.Wrapper[T, B]
	sizer      batch.Sizer[T]
	batchers   map[string]*batch.Batcher[T, B]
	serializer func(clientID string, batch B) (m.Message, error)
	maxWeight  int
}

func NewRoutedSender[T any, B any](
	exchange *m.ExchangeMiddleware,
	wrapper batch.Wrapper[T, B],
	sizer batch.Sizer[T],
	maxWeight int,
	serializer func(clientID string, batch B) (m.Message, error),
) *RoutedSender[T, B] {
	return &RoutedSender[T, B]{
		exchange:   exchange,
		wrapper:    wrapper,
		sizer:      sizer,
		maxWeight:  maxWeight,
		serializer: serializer,
		batchers:   make(map[string]*batch.Batcher[T, B]),
	}
}

func (s *RoutedSender[T, B]) Add(clientID string, item RoutedItem[T]) error {
	batcherKey := s.batcherKey(clientID, item.Route)
	batcher, exists := s.batchers[batcherKey]
	if !exists {
		newBatch := batch.New(s.maxWeight, s.sizer, s.wrapper)

		onFlush := func(batch B) error {
			return s.flushBatch(clientID, item.Route, batch)
		}

		batcher = batch.NewBatcher(newBatch, onFlush)
		s.batchers[batcherKey] = batcher
	}
	return batcher.Add(item.Item)
}

func (s *RoutedSender[T, B]) Flush(clientID string) error {
	for key, batcher := range s.batchers {
		if batcherClientID(key) != clientID {
			continue
		}
		if err := batcher.Flush(); err != nil {
			return err
		}
	}
	return nil
}

func (s *RoutedSender[T, B]) Cleanup(clientID string) error {
	for key := range s.batchers {
		if batcherClientID(key) == clientID {
			delete(s.batchers, key)
		}
	}
	return nil
}

func (s *RoutedSender[T, B]) SendEOF(clientID string, survivorCount uint64) error {
	eofInnerMsg := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: survivorCount,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofInnerMsg)
	if err != nil {
		return err
	}
	return s.exchange.Send(msg)
}

func (s *RoutedSender[T, B]) Close() error {
	for clientID := range s.batchers {
		delete(s.batchers, clientID)
	}
	return s.exchange.Close()
}

func (s *RoutedSender[T, B]) flushBatch(clientID string, route string, batch B) error {
	msg, err := s.serializer(clientID, batch)
	if err != nil {
		return err
	}
	return s.exchange.SendWithKey(route, msg)
}

func (s *RoutedSender[T, B]) batcherKey(clientID string, route string) string {
	return fmt.Sprintf("%s.%s", clientID, route)
}

func batcherClientID(key string) string {
	for i := range key {
		if key[i] == 0 {
			return key[:i]
		}
	}
	return key
}
