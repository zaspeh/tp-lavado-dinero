package engine

import (
	"log/slog"
	"sync/atomic"

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
	// TODO: cm.LoadState() -> no cargamos el estado del processor cuando se despierta
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
		slog.Debug("Data Message Received")
		return e.handleDataMessage(event.ClientID, event.EventID, event.Data)
	case r.EOFMessage:
		return e.coordinator.HandleLocalEOF(event.ClientID, event.EOFCount, event.EventID)
	case r.CleanupMessage:
		return e.handleCleanupMessage(event.ClientID)
	}
	return nil
}

func (e *StatefulEngine[T, V]) handleDataMessage(clientID, batchID string, data []T) error {
	// if e.checkpointManager != nil {
	// 	// TODO: ACKEAR
	// 	shouldProcess, err := e.checkpointManager.BeginBatch(clientID, batchID, func() {})
	// 	if err != nil {
	// 		return err
	// 	}
	// 	if !shouldProcess {
	// 		return nil
	// 	}
	// }

	for _, item := range data {
		if err := e.processor.Process(clientID, item, e.checkpointManager); err != nil {
			return err
		}
	}

	// if e.checkpointManager != nil {
	// 	e.checkpointManager.CommitBatch(clientID, batchID)
	// }
	return nil
}

func (e *StatefulEngine[T, V]) handleTrueEOF(clientID string, eofCount uint64, eofID string) error {
	// if e.checkpointManager != nil {
	// 	if err := e.checkpointManager.BeforeEOF(clientID); err != nil {
	// 		slog.Error("StatefulEngine: BeforeEOF failed", "error", err, "clientID", clientID)
	// 		return err
	// 	}
	// }

	yield := func(result V) error {
		return e.sender.Add(clientID, result, eofID)
	}

	survivors, err := e.processor.Finalize(clientID, yield)
	if err != nil {
		return err
	}

	if err := e.sender.Flush(clientID); err != nil {
		return err
	}

	if survivors == 0 {
		survivors = eofCount
	}

	if e.coordinator.IsLeader() {
		slog.Info("True EOF reached, sending EOF", "clientID", clientID, "survivorCount", survivors)
		return e.sender.SendEOF(clientID, survivors, eofID)
	}

	return nil
}

func (e *StatefulEngine[T, V]) handleCleanupMessage(clientID string) error {
	err := e.processor.Cleanup(clientID)
	// if e.checkpointManager != nil {
	// 	if clearErr := e.checkpointManager.ClearState(clientID); clearErr != nil {
	// 		slog.Warn("StatefulEngine: ClearState failed", "error", clearErr, "clientID", clientID)
	// 	}
	// }
	return err
}
