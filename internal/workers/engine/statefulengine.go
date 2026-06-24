package engine

import (
	"log/slog"
	"sync/atomic"
	"time"

	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
	c "github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	p "github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	r "github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	s "github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type StatefulEngine[T any, V any] struct {
	receiver          r.Receiver[T]
	sender            s.Sender[V]
	processor         p.StatefulProcessor[T, V]
	coordinator       c.Coordinator
	checkpointManager *checkpoint.CheckpointManager
	wasStopped        atomic.Bool
}

func NewStatefulEngine[T any, V any](receiver r.Receiver[T], sender s.Sender[V], processor p.StatefulProcessor[T, V], coordinator c.Coordinator, cm *checkpoint.CheckpointManager) *StatefulEngine[T, V] {
	engine := &StatefulEngine[T, V]{
		receiver:          receiver,
		sender:            sender,
		processor:         processor,
		coordinator:       coordinator,
		checkpointManager: cm,
	}
	engine.coordinator.SetFlushHandler(engine.handleTrueEOF)
	return engine
}

func (e *StatefulEngine[T, V]) Run() error {
	if e.wasStopped.Load() {
		return nil
	}

	e.handleEofRecovery()

	return e.receiver.Receive(e.handleEvent)
}

func (e *StatefulEngine[T, V]) handleEofRecovery() {
	clients := e.checkpointManager.GetClientsNeedingFinalize()
	for _, clientID := range clients {
		slog.Info("StatefulEngine: recovering EOF for client", "clientID", clientID)
		e.handleTrueEOF(clientID, 0, "recovery-eof-id")
	}
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
		slog.Debug("Data Message Received")
		return e.handleDataMessage(event)
	case r.EOFMessage:
		if err := e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount, event.EventID); err != nil {
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
			return err
		}
	}

	if err := e.checkpointManager.CommitBatch(clientID, batchID); err != nil {
		return err
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleTrueEOF(clientID string, eofCount uint64, eofID string) error {
	if e.checkpointManager.NeedsFinalize(clientID) {
		if err := e.checkpointManager.BeforeEOF(clientID); err != nil {
			slog.Error("StatefulEngine: BeforeEOF failed", "error", err, "clientID", clientID)
			return err
		}

		if err := e.checkpointManager.SetEofSent(clientID); err != nil {
			slog.Error("StatefulEngine: SetEofSent failed", "error", err, "clientID", clientID)
			return err
		}

		slog.Info("True EOF reached, DEBUG: sleeping 10s before SendEOF", "clientID", clientID)
		time.Sleep(10 * time.Second)
		yield := func(result V) error {
			return e.sender.Add(clientID, result, eofID)
		}

		survivors, err := e.processor.Finalize(clientID, yield)
		if err != nil {
			return err
		}

		e.checkpointManager.SetFinalizeComplete(clientID)

		if err := e.sender.Flush(clientID); err != nil {
			return err
		}

		if e.coordinator.IsLeader() {
			slog.Info("True EOF reached, sending EOF", "clientID", clientID, "survivorCount", survivors)
			return e.sender.SendEOF(clientID, survivors, eofID)
		}
	} else {
		slog.Info("StatefulEngine handleTrueEOF: client already finalized, skipping", "clientID", clientID, "eofID", eofID)
	}

	return nil
}

func (e *StatefulEngine[T, V]) handleCleanupMessage(clientID string) error {
	if err := e.processor.Cleanup(clientID); err != nil {
		return err
	}
	if err := e.checkpointManager.ClearState(clientID); err != nil {
		slog.Warn("StatefulEngine: ClearState failed", "error", err, "clientID", clientID)
	}
	return nil
}
