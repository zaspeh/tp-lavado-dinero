package engine

import (
	"fmt"
	"log/slog"
	"sync/atomic"

	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatefulEngine[T any, V any] struct {
	id                int
	receiver          r.Receiver[T]
	sender            s.Sender[V]
	processor         p.StatefulProcessor[T, V]
	coordinator       c.Coordinator
	checkpointManager *checkpoint.CheckpointManager
	wasStopped        atomic.Bool
	cleanupMessages   map[string]bool
}

func NewStatefulEngine[T any, V any](workerId int, receiver r.Receiver[T], sender s.Sender[V], processor p.StatefulProcessor[T, V], coordinator c.Coordinator, cm *checkpoint.CheckpointManager) *StatefulEngine[T, V] {
	engine := &StatefulEngine[T, V]{
		id:                workerId,
		receiver:          receiver,
		sender:            sender,
		processor:         processor,
		coordinator:       coordinator,
		checkpointManager: cm,
		cleanupMessages:   make(map[string]bool),
	}
	engine.coordinator.SetFlushHandler(engine.handleTrueEOF)
	return engine
}

func (e *StatefulEngine[T, V]) Run() error {
	if e.wasStopped.Load() {
		return nil
	}

	e.checkpointManager.LoadState(e.coordinator)
	go e.coordinator.Run()
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
	if e.cleanupMessages[event.ClientID] == true {
		slog.Debug("Aceking old client message: ", "ClientID", event.ClientID)
		event.AckFn()
		return nil
	}
	switch event.Type {
	case r.DataMessage:
		slog.Debug("Data Message Received")
		return e.handleDataMessage(event)
	case r.EOFMessage:
		if err := e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount, event.EventID); err != nil {
			return err
		}
		if err := e.checkpointManager.FlushPendingBatches(e.coordinator, event.ClientID); err != nil {
			return err
		}
		e.checkpointManager.AckEOF(event.AckFn)
		return nil
	case r.CleanupMessage:
		if err := e.handleCleanupMessage(event.ClientID); err != nil {
			return err
		}
		e.checkpointManager.AckCleanup(event.AckFn)
		return nil
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleDataMessage(event r.Event[T]) error {
	clientID := event.ClientID
	batchID := event.EventID
	data := event.Data

	// El checkpoint manager es el único responsable del ack/nack:
	// - Si el batch ya fue procesado (cargado de disco), dispara el ack inmediato.
	// - Si es nuevo, acumula el ack y lo dispara cuando persistAndAck escribe a disco.
	shouldProcess, err := e.checkpointManager.BeginBatch(clientID, batchID, event.AckFn)
	if err != nil {
		return err
	}
	if !shouldProcess {
		return nil
	}

	for _, item := range data {
		if err := e.processor.Process(clientID, item, e.checkpointManager); err != nil {
			e.checkpointManager.AbortBatch(clientID)
			return err
		}
	}

	processed := uint64(len(data))
	if err := e.checkpointManager.CommitBatch(clientID, batchID, processed, e.coordinator); err != nil {
		return err
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleTrueEOF(clientID string, _ uint64, eofID string) error {
	newEof := fmt.Sprintf("%s-%d", eofID, e.id)
	yield := func(result V) error {
		return e.sender.Add(clientID, result, newEof)
	}

	survivors, err := e.processor.Finalize(clientID, yield)
	if err != nil {
		return err
	}

	if err := e.sender.Flush(clientID); err != nil {
		return err
	}

	slog.Info("True EOF reached, sending EOF", "clientID", clientID, "survivorCount", survivors, "eofID", newEof)

	return e.sender.SendEOF(clientID, survivors, newEof)
}

func (e *StatefulEngine[T, V]) handleCleanupMessage(clientID string) error {
	if err := e.processor.Cleanup(clientID); err != nil {
		return err
	}

	if err := e.coordinator.ClearClient(clientID); err != nil {
		slog.Warn("StatelessEngine: coordinator.ClearClient failed", "error", err, "clientID", clientID)
	}

	if err := e.sender.Cleanup(clientID); err != nil {
		slog.Warn("StatelessEngine: sender.Cleanup failed", "error", err, "clientID", clientID)
	}

	if err := e.checkpointManager.ClearState(clientID); err != nil {
		slog.Warn("StatelessEngine: checkpointManager.ClearState failed", "error", err, "clientID", clientID)
	}

	slog.Info("Cleanup for client: ", "clientID", clientID)
	e.cleanupMessages[clientID] = true

	return nil
}
