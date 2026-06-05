package engine

import (
	"sync/atomic"

	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatefulEngine[T any, V any] struct {
	receiver   r.Receiver[T]
	sender     s.Sender[V]
	processor  p.Processor[T, V]
	wasStopped atomic.Bool
}

func NewStatefulEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], processor p.Processor[T, V]) (*StatefulEngine[T, V], error) {
	engine := &StatefulEngine[T, V]{
		receiver:  receiver,
		sender:    sender,
		processor: processor,
	}

	return engine, nil
}

func (e *StatefulEngine[T, V]) Run() error {
	if e.wasStopped.Load() {
		return nil
	}
	return e.receiver.Receive(e.handleEvent)
}

func (e *StatefulEngine[T, V]) Shutdown() {
	if e.wasStopped.Load() {
		return
	}

	e.wasStopped.Store(true)
	e.receiver.Close()
	e.sender.Close()
}

func (e *StatefulEngine[T, V]) handleEvent(event r.Event[T]) error {
	switch event.Type {
	case r.DataMessage:
		return e.handleDataMessage(event.ClientID, event.Data)
	case r.EOFMessage:
		return e.handleEOFMessage(event.ClientID, event.EOFCount)
	case r.CleanupMessage:
		// return e.coordinator.HandleCleanup(event.ClientID)
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleDataMessage(clientID string, data []T) error {
	for _, item := range data {
		_, err := e.processor.Process(clientID, item)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleEOFMessage(clientID string, eofCount uint64) error {
	// return e.coordinator.HandleLocalEOF(clientID, eofCount)
	return nil
}
