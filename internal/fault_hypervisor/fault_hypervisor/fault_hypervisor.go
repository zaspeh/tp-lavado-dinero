package faulthypervisor

import (
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	composeLoader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/compose_loader"
)

type WorkerStatus struct {
	ContainerName string
	LastSeen      time.Time
	IsAlive       bool
}

type FaultHypervisorConfig struct {
	ConnectionSettings      middleware.ConnSettings
	HeartbeatQueueName      string
	CheckIntervalSeconds    int
	HeartbeatTimeoutSeconds int
}

type FaultHypervisor struct {
	HeartbeatQueue          middleware.Middleware
	CheckIntervalSeconds    int
	HeartbeatTimeoutSeconds int
	workers                 map[string]*WorkerStatus
	mu                      sync.RWMutex
}

func NewFaultHypervisor(config FaultHypervisorConfig) (*FaultHypervisor, error) {
	heartbeatQueue, err := middleware.CreateQueueMiddleware(
		config.HeartbeatQueueName,
		config.ConnectionSettings,
	)
	if err != nil {
		return nil, err
	}

	workers, err := composeLoader.LoadWorkersFromCompose("/app/Compose.yml")
	if err != nil {
		return nil, err
	}

	workerMap := make(map[string]*WorkerStatus)

	for _, worker := range workers {
		slog.Debug("worker loaded from compose", "container_name", worker.ContainerName)
		workerMap[worker.ContainerName] = &WorkerStatus{
			ContainerName: worker.ContainerName,
			IsAlive:       true,
			LastSeen:      time.Now(),
		}
	}

	return &FaultHypervisor{
		HeartbeatQueue: heartbeatQueue,

		CheckIntervalSeconds:    config.CheckIntervalSeconds,
		HeartbeatTimeoutSeconds: config.HeartbeatTimeoutSeconds,

		workers: workerMap,
	}, nil
}

func (fh *FaultHypervisor) Run() error {
	go fh.handleSignals()
	go fh.monitorWorkers()

	slog.Info("starting consuming heartbeats")

	return fh.HeartbeatQueue.StartConsuming(
		func(msg middleware.Message, ack, nack func()) {
			slog.Debug("message received")
			fh.handleHeartbeat(msg, ack, nack)
		},
	)
}

func (fh *FaultHypervisor) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("fault hypervisor shutting down")
	fh.HeartbeatQueue.Close()
}

func (fh *FaultHypervisor) handleHeartbeat(msg middleware.Message, ack, nack func()) {
	slog.Debug("entered handleHeartbeat")
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	heartbeat := moneyLaundry.GetHeartbeat()
	if heartbeat == nil {
		nack()
		return
	}

	containerName := heartbeat.GetContainerName()

	fh.mu.Lock()

	worker, exists := fh.workers[containerName]

	if !exists {
		fh.mu.Unlock()
		slog.Warn("heartbeat from unknown worker", "container", containerName)
		nack()
		return
	}

	worker.LastSeen = time.Now()
	worker.IsAlive = true

	fh.mu.Unlock()

	slog.Debug("heartbeat received", "container_name", worker.ContainerName)
	ack()
}

func (fh *FaultHypervisor) monitorWorkers() {
	ticker := time.NewTicker(
		time.Duration(fh.CheckIntervalSeconds) * time.Second,
	)
	defer ticker.Stop()

	for range ticker.C {
		fh.checkWorkers()
	}
}

func (fh *FaultHypervisor) checkWorkers() {
	timeout := time.Duration(
		fh.HeartbeatTimeoutSeconds,
	) * time.Second

	now := time.Now()

	var deadWorkers []*WorkerStatus

	fh.mu.Lock()

	for _, worker := range fh.workers {
		if worker.IsAlive && now.Sub(worker.LastSeen) > timeout {
			worker.IsAlive = false
			deadWorkers = append(deadWorkers, worker)
		}
	}

	fh.mu.Unlock()

	for _, worker := range deadWorkers {
		fh.markWorkerDead(worker)
	}
}

func (fh *FaultHypervisor) markWorkerDead(worker *WorkerStatus) {
	slog.Warn("worker marked as dead", "container_name", worker.ContainerName)

	fh.reviveWorker(worker)
}

func (fh *FaultHypervisor) reviveWorker(worker *WorkerStatus) {
	slog.Info("would restart worker", "container", worker.ContainerName)
}
