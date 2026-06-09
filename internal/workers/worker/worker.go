package worker

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/heartbeat"
)

type Worker struct {
	engines            []e.Engine
	heartbeatPublisher *heartbeat.HeartbeatPublisher
}

func NewWorker(heartbeatPublisher *heartbeat.HeartbeatPublisher) *Worker {
	return &Worker{
		engines:            []e.Engine{},
		heartbeatPublisher: heartbeatPublisher,
	}
}

func (w *Worker) AddEngine(engine e.Engine) {
	w.engines = append(w.engines, engine)
}

func (w *Worker) Run() error {
	go w.handleSignals()

	// Canal para almacenar errores de los engines
	errChan := make(chan error, len(w.engines))

	// Levantamos todos los engines en rutinas separadas
	var wg sync.WaitGroup
	for _, e := range w.engines {
		wg.Go(func() {
			if err := e.Run(); err != nil {
				errChan <- err
				w.shutdown() // Si un engine falla, apagamos todo el worker
			}
		})
	}

	// Corremos el heartbeat en paralelo una vez levantamos los engines
	go w.heartbeatPublisher.Run()

	wg.Wait()
	close(errChan)
	return <-errChan
}

// TODO: hacer log de los close fallidos.
func (w *Worker) shutdown() {
	for _, e := range w.engines {
		e.Shutdown()
	}
	w.heartbeatPublisher.Close()
}

func (w *Worker) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	w.shutdown()
}
