package faulthypervisor

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type WorkerStatus struct {
	WorkerID   int64
	WorkerType string
	LastSeen   time.Time
	IsAlive    bool
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
	lastSeen                map[string]*WorkerStatus
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

	return &FaultHypervisor{
		HeartbeatQueue: heartbeatQueue,

		CheckIntervalSeconds:    config.CheckIntervalSeconds,
		HeartbeatTimeoutSeconds: config.HeartbeatTimeoutSeconds,

		lastSeen: make(map[string]*WorkerStatus),
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

	workerID := heartbeat.GetWorkerId()
	parsedWorkerID := strconv.FormatInt(workerID, 10)
	worker_name := heartbeat.GetWorkerType() + "-" + parsedWorkerID

	slog.Debug(
		"processing heartbeat",
		"worker_id", parsedWorkerID,
		"worker_type", heartbeat.GetWorkerType(),
	)

	fh.mu.Lock()
	fh.lastSeen[worker_name] = &WorkerStatus{
		WorkerID:   workerID,
		WorkerType: heartbeat.GetWorkerType(),
		LastSeen:   time.Now(),
		IsAlive:    true,
	}
	fh.mu.Unlock()

	slog.Debug(
		"heartbeat received",
		"worker_id", parsedWorkerID,
		"worker_type", heartbeat.GetWorkerType(),
	)

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

	fh.mu.RLock()
	defer fh.mu.RUnlock()

	for workerID, workerStatus := range fh.lastSeen {
		if workerStatus.IsAlive && now.Sub(workerStatus.LastSeen) > timeout {
			workerStatus.IsAlive = false

			slog.Warn(
				"worker timeout detected",
				"worker", workerID,
				"last_seen", workerStatus.LastSeen,
			)
		}
	}
}
