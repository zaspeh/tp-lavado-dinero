package sender

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type PublishFunc func(clientID string, msg m.Message) error

type senderExchange interface {
	m.Middleware
	SendWithKey(string, m.Message) error
}

type SingleSender[T any, V any] struct {
	namespace  string
	output     m.Middleware
	wrapper    batch.Wrapper[T, V]
	sizer      batch.Sizer[T]
	batchers   map[string]*batch.Batcher[T, V]
	serializer SerializerFunc[V]
	maxWeight  int
	publish    PublishFunc
}

func NewSingleSender[T any, V any](output m.Middleware, wrapper batch.Wrapper[T, V],
	sizer batch.Sizer[T], maxWeight int, serializer SerializerFunc[V], namespace string,
) *SingleSender[T, V] {
	sender := &SingleSender[T, V]{
		namespace:  namespace,
		output:     output,
		wrapper:    wrapper,
		sizer:      sizer,
		maxWeight:  maxWeight,
		serializer: serializer,
		batchers:   make(map[string]*batch.Batcher[T, V]),
	}

	sender.publish = func(_ string, msg m.Message) error {
		return output.Send(msg)
	}

	return sender
}

func NewDynamicKeySender[T any, V any](output senderExchange, keyResolver func(clientID string) string, wrapper batch.Wrapper[T, V],
	sizer batch.Sizer[T], maxWeight int, serializer SerializerFunc[V], namespace string,
) *SingleSender[T, V] {
	sender := NewSingleSender(output, wrapper, sizer, maxWeight, serializer, namespace)

	sender.publish = func(clientID string, msg m.Message) error {
		return output.SendWithKey(
			keyResolver(clientID),
			msg,
		)
	}

	return sender
}

func (s *SingleSender[T, V]) Add(clientID string, item T, batchID string) error {
	batcher, exists := s.batchers[clientID]
	if !exists {
		newBatch := batch.New(s.maxWeight, s.sizer, s.wrapper)

		onFlush := func(batch V, batchID string) error {
			return s.flushBatch(clientID, batch, batchID)
		}

		batcher = batch.NewBatcher(newBatch, onFlush)
		s.batchers[clientID] = batcher
	}

	batchID = namespacedID(batchID, s.namespace)
	batcher.SetNewBatchId(batchID)
	return batcher.Add(item)
}

func (s *SingleSender[T, V]) Flush(clientID string) error {
	if batcher, exists := s.batchers[clientID]; exists {
		return batcher.Flush()
	}
	return nil
}

func (s *SingleSender[T, V]) Cleanup(clientID string) error {
	cleanupMsg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_CLEANUP, nil, "")
	if err != nil {
		return err
	}
	if err := s.publish(clientID, cleanupMsg); err != nil {
		return err
	}
	delete(s.batchers, clientID)
	return nil
}

func (s *SingleSender[T, V]) SendEOF(clientID string, survivorCount uint64, eofID string) error {
	eofID = namespacedID(eofID, s.namespace)
	eofInnerMsg := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: survivorCount,
		},
	}

	msg, err := protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofInnerMsg, eofID)
	if err != nil {
		return err
	}
	return s.publish(clientID, msg)
}

func namespacedID(id, namespace string) string {
	return fmt.Sprintf("%s-%s", id, namespace)
}

func (s *SingleSender[T, V]) Close() error {
	for clientID := range s.batchers {
		s.Cleanup(clientID)
	}
	return s.output.Close()
}

func (s *SingleSender[T, V]) flushBatch(clientID string, batch V, batchID string) error {
	msg, err := s.serializer(clientID, batchID, batch)
	if err != nil {
		return err
	}
	return s.publish(clientID, msg)
}
