package worker

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"

	e "github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
)

type Worker struct {
	engines []e.Engine
}

func NewWorker() *Worker {
	return &Worker{
		engines: []e.Engine{},
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

	wg.Wait()
	close(errChan)
	return <-errChan
}

func (w *Worker) shutdown() {
	for _, e := range w.engines {
		e.Shutdown()
	}
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
