package engine

import (
	"sync/atomic"

	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/eofcoordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/procesor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatelessEngine[T any, V any] struct {
	receiver    r.Receiver[T]
	sender      s.Sender[V]
	coordinator *c.EOFCoordinator
	procesor    p.Procesor[T, V]
	wasStopped  atomic.Bool
}

func NewStatelessEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], procesor p.Procesor[T, V], coordinator *c.EOFCoordinator) *StatelessEngine[T, V] {
	return &StatelessEngine[T, V]{
		receiver:    receiver,
		sender:      sender,
		coordinator: coordinator,
		procesor:    procesor,
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
		return e.handleDataMessage(event.ClientID, event.Data)
	case r.EOFMessage:
		return e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount)
	case r.CleanupMessage:
		// e.coordinator.MarkCleanup(event.ClientID)
	}
	return nil
}

func (e *StatelessEngine[T, V]) handleDataMessage(clientID string, data []T) error {
	for _, item := range data {
		result, err := e.procesor.Process(clientID, item)
		if err != nil {
			return err
		}

		if err := e.sender.Add(clientID, result); err != nil {
			return err
		}
	}
	return nil
}
