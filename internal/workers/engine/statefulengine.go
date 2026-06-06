package engine

import (
	"log/slog"
	"sync/atomic"

	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatefulEngine[T any, V any] struct {
	receiver    r.Receiver[T]
	sender      s.Sender[V]
	processor   p.StatefulProcessor[T, V]
	coordinator c.Coordinator
	wasStopped  atomic.Bool
}

func NewStatefulEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], processor p.StatefulProcessor[T, V], coordinator c.Coordinator) *StatefulEngine[T, V] {
	engine := &StatefulEngine[T, V]{
		receiver:    receiver,
		sender:      sender,
		processor:   processor,
		coordinator: coordinator,
	}

	engine.coordinator.SetFlushHandler(engine.handleTrueEOF)
	return engine
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
	e.coordinator.Close()
}

func (e *StatefulEngine[T, V]) handleEvent(event r.Event[T]) error {
	switch event.Type {
	case r.DataMessage:
		return e.handleDataMessage(event.ClientID, event.Data)
	case r.EOFMessage:
		return e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount)
	case r.CleanupMessage:
		return e.handleCleanupMessage(event.ClientID)
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleDataMessage(clientID string, data []T) error {
	for _, item := range data {
		if err := e.processor.Process(clientID, item); err != nil {
			return err
		}
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleTrueEOF(clientID string, eofCount uint64) error {
	yield := func(result V) error {
		return e.sender.Add(clientID, result)
	}

	if err := e.processor.Finalize(clientID, yield); err != nil {
		return err
	}

	if err := e.sender.Flush(clientID); err != nil {
		return err
	}

	if e.coordinator.IsLeader() {
		slog.Info("True EOF reached, sending EOF", "clientID", clientID, "survivorCount", eofCount)
		return e.sender.SendEOF(clientID, eofCount)
	}

	return nil
}

func (e *StatefulEngine[T, V]) handleCleanupMessage(clientID string) error {
	return e.processor.Cleanup(clientID)
	//TODO: SEGUIR ENVIANDO CLEANUP AL SIGUIENTE
}
