package faulthypervisor

import (
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"
	runtimepkg "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/runtime"
)

type WorkerStatus struct {
	ContainerName string
	WorkerID      int
	Definition    configloader.WorkerDefinition
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
	runtime                 runtimepkg.Runtime
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

	workerDefinitions, err := configloader.LoadWorkersFromConfig("/app/config.yml")
	if err != nil {
		return nil, err
	}

	workerMap := make(map[string]*WorkerStatus)

	for _, definition := range workerDefinitions {
		for i := 0; i < definition.Count; i++ {

			containerName := fmt.Sprintf("%s_%d", definition.ServiceName, i)

			slog.Info(
				"worker loaded from config",
				"container_name",
				containerName,
				"worker_type",
				definition.WorkerType,
			)

			workerMap[containerName] = &WorkerStatus{
				ContainerName: containerName,
				WorkerID:      i,
				Definition:    definition,
				IsAlive:       false,
			}
		}
	}

	return &FaultHypervisor{
		HeartbeatQueue: heartbeatQueue,

		CheckIntervalSeconds:    config.CheckIntervalSeconds,
		HeartbeatTimeoutSeconds: config.HeartbeatTimeoutSeconds,

		workers: workerMap,
		runtime: runtimepkg.NewDockerRuntime(),
	}, nil
}

func (fh *FaultHypervisor) Run() error {
	slog.Info("creating worker network")

	if err := fh.runtime.EnsureNetwork("money_laundering_network"); err != nil {
		return err
	}

	imageExists, err := fh.runtime.ImageExists("tp-worker")
	if err != nil {
		return err
	}

	if !imageExists {
		slog.Info("building worker image")

		if err := fh.runtime.BuildWorkerImage(); err != nil {
			return err
		}

		slog.Info("worker image built")
	}

	if err := fh.StartWorkers(); err != nil {
		return err
	}

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
	slog.Info("restarting worker", "container", worker.ContainerName)

	err := fh.runtime.RestartWorker(worker.ContainerName)
	if err != nil {
		slog.Error("restart failed", "container", worker.ContainerName, "error", err)
		return
	}

	slog.Info("worker restarted", "container", worker.ContainerName)
}

func (fh *FaultHypervisor) StartWorkers() error {
	for _, worker := range fh.workers {
		exists, err := fh.runtime.ContainerExists(worker.ContainerName)
		if err != nil {
			return err
		}

		if exists {
			continue
		}

		slog.Info(
			"creating worker",
			"container",
			worker.ContainerName,
		)

		err = fh.runtime.CreateWorker(worker.ContainerName, worker.WorkerID, worker.Definition)
		if err != nil {
			return err
		}
	}

	return nil
}
