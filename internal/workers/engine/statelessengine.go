package engine

import (
	"log/slog"
	"sync/atomic"

	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatelessEngine[T any, V any] struct {
	receiver    r.Receiver[T]
	sender      s.Sender[V]
	coordinator c.Coordinator
	processor   p.Processor[T, V]
	wasStopped  atomic.Bool
}

func NewStatelessEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], processor p.Processor[T, V], coordinator c.Coordinator) *StatelessEngine[T, V] {
	engine := &StatelessEngine[T, V]{
		receiver:    receiver,
		sender:      sender,
		processor:   processor,
		coordinator: coordinator,
	}
	engine.coordinator.SetFlushHandler(engine.handleTrueEOF)
	return engine
}

func (e *StatelessEngine[T, V]) Run() error {
	if e.wasStopped.Load() {
		return nil
	}
	go e.coordinator.Run()
	return e.receiver.Receive(e.handleEvent)
}

func (e *StatelessEngine[T, V]) Shutdown() {
	if e.wasStopped.Load() {
		return
	}

	e.wasStopped.Store(true)
	e.receiver.Close()
	e.sender.Close()
	e.coordinator.Close()
}

func (e *StatelessEngine[T, V]) handleEvent(event r.Event[T]) error {
	switch event.Type {
	case r.DataMessage:
		slog.Debug("Data message received by pipeline")
		return e.handleDataMessage(event.ClientID, event.Data, event.EventID)
	case r.EOFMessage:
		slog.Debug("EOF received by pipeline")
		return e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount, event.EventID)
	case r.CleanupMessage:
		// e.coordinator.MarkCleanup(event.ClientID)
	}
	return nil
}

func (e *StatelessEngine[T, V]) handleDataMessage(clientID string, data []T, batchID string) error {
	for _, item := range data {
		results, err := e.processor.Process(clientID, item)
		if err != nil {
			return err
		}
		for _, result := range results {
			if err := e.sender.Add(clientID, result, batchID); err != nil {
				return err
			}
		}

		if len(results) > 0 {
			if err := e.coordinator.RecordSurvivor(clientID); err != nil {
				return err
			}
		}

		if err := e.coordinator.RecordProcessed(clientID); err != nil {
			return err
		}
	}

	return e.sender.Flush(clientID)
}

func (e *StatelessEngine[T, V]) handleTrueEOF(clientID string, survivorCount uint64, eofID string) error {
	if !e.coordinator.IsLeader() {
		return nil
	}
	slog.Info("True EOF reached, sending EOF", "clientID", clientID, "survivorCount", survivorCount)
	return e.sender.SendEOF(clientID, survivorCount, eofID)
}
