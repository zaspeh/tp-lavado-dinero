package sender

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
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
	serializer SerializerFunc[B]
	maxWeight  int
}

func NewRoutedSender[T any, B any](
	exchange *m.ExchangeMiddleware,
	wrapper batch.Wrapper[T, B],
	sizer batch.Sizer[T],
	maxWeight int,
	serializer SerializerFunc[B],
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

func (s *RoutedSender[T, B]) Add(clientID string, item RoutedItem[T], batchID string) error {
	batcherKey := s.batcherKey(clientID, item.Route)
	batcher, exists := s.batchers[batcherKey]
	if !exists {
		newBatch := batch.New(s.maxWeight, s.sizer, s.wrapper)

		onFlush := func(batch B, batchID string) error {
			return s.flushBatch(clientID, item.Route, batch, batchID)
		}

		batcher = batch.NewBatcher(newBatch, onFlush)
		s.batchers[batcherKey] = batcher
	}
	batcher.SetNewBatchId(batchID)
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

func (s *RoutedSender[T, B]) SendEOF(clientID string, survivorCount uint64, eofID string) error {
	eofInnerMsg := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: survivorCount,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofInnerMsg, eofID)
	if err != nil {
		return err
	}
	return s.exchange.Send(msg)
}

func (s *RoutedSender[T, B]) Close() error {
	for clientID := range s.batchers {
		s.Cleanup(clientID)
	}
	return s.exchange.Close()
}

func (s *RoutedSender[T, B]) flushBatch(clientID string, route string, batch B, batchID string) error {
	msg, err := s.serializer(clientID, batchID, batch)
	if err != nil {
		return err
	}
	return s.exchange.SendWithKey(route, msg)
}

func (s *RoutedSender[T, B]) batcherKey(clientID string, route string) string {
	return fmt.Sprintf("%s\x00%s", clientID, route)
}

func batcherClientID(key string) string {
	for i := range key {
		if key[i] == 0 {
			return key[:i]
		}
	}
	return key
}
