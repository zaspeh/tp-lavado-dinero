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
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"
	runtimepkg "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/runtime"
)

const configPath = "/app/config.yml"

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
	RuntimeConfig           runtimepkg.RuntimeConfig
}

type FaultHypervisor struct {
	HeartbeatQueue          middleware.Middleware
	CheckIntervalSeconds    int
	HeartbeatTimeoutSeconds int
	workers                 map[string]*WorkerStatus
	runtime                 runtimepkg.Runtime
	mu                      sync.RWMutex
	ready                   bool
}

func NewFaultHypervisor(config FaultHypervisorConfig) (*FaultHypervisor, error) {
	heartbeatQueue, err := middleware.CreateQueueMiddleware(
		config.HeartbeatQueueName,
		config.ConnectionSettings,
	)
	if err != nil {
		return nil, err
	}

	workers, err := loadWorkers()
	if err != nil {
		return nil, err
	}

	runtime, err := runtimepkg.NewDockerRuntime(config.RuntimeConfig)
	if err != nil {
		return nil, err
	}

	return &FaultHypervisor{
		HeartbeatQueue: heartbeatQueue,

		CheckIntervalSeconds:    config.CheckIntervalSeconds,
		HeartbeatTimeoutSeconds: config.HeartbeatTimeoutSeconds,

		workers: workers,
		runtime: runtime,
	}, nil
}

func (fh *FaultHypervisor) Run() error {
	if err := fh.initializeRuntime(); err != nil {
		return err
	}

	if err := fh.StartWorkers(); err != nil {
		return err
	}

	if err := os.WriteFile("/tmp/ready", []byte("ready"), 0644); err != nil {
		slog.Error("failed to create ready file", "error", err)
		return err
	}

	fh.startBackgroundTasks()

	slog.Info("starting consuming heartbeats")

	return fh.consumeHeartbeats()
}

func (fh *FaultHypervisor) startBackgroundTasks() {
	go fh.handleSignals()
	go fh.monitorWorkers()
}

func (fh *FaultHypervisor) consumeHeartbeats() error {
	return fh.HeartbeatQueue.StartConsuming(
		func(
			msg middleware.Message,
			ack,
			nack func(),
		) {
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
	heartbeat, err := extractHeartbeat(msg)

	if err != nil {
		nack()
		return
	}

	if !fh.registerHeartbeat(
		heartbeat.GetContainerName(),
	) {
		nack()
		return
	}

	ack()
}

func extractHeartbeat(msg middleware.Message) (*protobuf.Heartbeat, error) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		return nil, err
	}

	heartbeat := moneyLaundry.GetHeartbeat()

	if heartbeat == nil {
		return nil, fmt.Errorf("message is not a heartbeat")
	}

	return heartbeat, nil
}

func (fh *FaultHypervisor) registerHeartbeat(containerName string) bool {

	fh.mu.Lock()
	defer fh.mu.Unlock()

	worker, exists := fh.workers[containerName]

	if !exists {
		slog.Warn("heartbeat from unknown worker", "container", containerName)

		return false
	}

	worker.LastSeen = time.Now()
	worker.IsAlive = true

	return true
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
		} else if !worker.IsAlive {
			deadWorkers = append(deadWorkers, worker)
		}
	}

	fh.mu.Unlock()

	for _, worker := range deadWorkers {
		fh.handleDeadWorker(worker)
	}
}

func (fh *FaultHypervisor) handleDeadWorker(worker *WorkerStatus) {
	slog.Warn("worker marked as dead", "container", worker.ContainerName)

	// TODO: Podría ocurrir que se reviva infinitamente si el worker tira panic.
	if err := fh.runtime.RestartWorker(worker.ContainerName); err != nil {
		slog.Error(
			"restart failed",
			"container",
			worker.ContainerName,
			"error",
			err,
		)
		return
	}

	fh.mu.Lock()
	worker.IsAlive = true
	fh.mu.Unlock()

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

func loadWorkers() (map[string]*WorkerStatus, error) {
	definitions, err := configloader.LoadWorkersFromConfig(configPath)

	if err != nil {
		return nil, err
	}

	workers := make(map[string]*WorkerStatus)

	for _, definition := range definitions {
		for i := 0; i < definition.Count; i++ {

			containerName := fmt.Sprintf(
				"%s_%d",
				definition.ServiceName,
				i,
			)

			workers[containerName] = &WorkerStatus{
				ContainerName: containerName,
				WorkerID:      i,
				Definition:    definition,
			}

			slog.Info(
				"worker loaded from config",
				"container_name",
				containerName,
				"worker_type",
				definition.WorkerType,
			)
		}
	}

	return workers, nil
}

func (fh *FaultHypervisor) initializeRuntime() error {
	slog.Info("creating worker network")

	if err := fh.runtime.EnsureNetwork(); err != nil {
		return err
	}

	exists, err := fh.runtime.ImageExists()

	if err != nil {
		return err
	}

	if exists {
		return nil
	}

	slog.Info("building worker image")

	if err := fh.runtime.BuildWorkerImage(); err != nil {
		return err
	}

	slog.Info("worker image built")

	return nil
}
