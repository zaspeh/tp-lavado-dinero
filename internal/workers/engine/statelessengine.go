package engine

import (
	"sync/atomic"

	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatelessEngine[T any, V any] struct {
	receiver    r.Receiver[T]
	sender      s.Sender[V]
	coordinator *c.EOFCoordinator
	wasStopped  atomic.Bool
}

func NewStatelessEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], coordinator *c.EOFCoordinator) *StatelessEngine[T, V] {
	return &StatelessEngine[T, V]{
		receiver:    receiver,
		sender:      sender,
		coordinator: coordinator,
	}
}

func (e *StatelessEngine[T, V]) Run() error {
	if e.wasStopped.Load() {
		return nil
	}
	return e.receiver.Receive(e.handleEvent)
}

func (e *StatelessEngine[T, V]) Shutdown() {
	e.wasStopped.Store(true)
	e.receiver.Close()
	e.sender.Close()
}

func (e *StatelessEngine[T, V]) handleEvent(event r.Event[T]) error {
	switch event.Type {
	case r.DataMessage:
		// Process data message and send results
		// result := processData(event.Data)
		// return e.sender.Add(event.ClientID, result)
	case r.EOFMessage:
		return e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount)
	case r.CleanupMessage:
		// e.coordinator.MarkCleanup(event.ClientID)
	}
	return nil
}
