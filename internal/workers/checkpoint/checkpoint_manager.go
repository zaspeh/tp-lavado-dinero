package checkpoint

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
)

type CheckpointManager struct {
	workerName             string
	workerID               int
	storage                *coordinator.BatchStorage
	processor              Checkpointable
	checkpointEveryBatches int

	pendingAcks      map[string][]func()
	batchCount       map[string]int
	processedBatches map[string]map[string]bool
	mu               sync.Mutex
}

func NewCheckpointManager(processor Checkpointable, storage *coordinator.BatchStorage) *CheckpointManager {
	return &CheckpointManager{
		workerName:             processor.GetWorkerName(),
		workerID:               processor.GetWorkerID(),
		storage:                storage,
		processor:              processor,
		checkpointEveryBatches: 100,
		pendingAcks:            make(map[string][]func()),
		batchCount:             make(map[string]int),
		processedBatches:       make(map[string]map[string]bool),
	}
}

func (cm *CheckpointManager) SetCheckpointEveryBatches(n int) {
	cm.checkpointEveryBatches = n
}

func (cm *CheckpointManager) LoadState() error {
	slog.Debug("CheckpointManager LoadState starting", "workerName", cm.workerName, "workerID", cm.workerID)

	if err := os.MkdirAll(cm.stateDir(), 0755); err != nil {
		return err
	}

	stateFiles, err := os.ReadDir(cm.stateDir())
	if err != nil {
		if os.IsNotExist(err) {
			slog.Debug("CheckpointManager LoadState: no state dir", "workerName", cm.workerName, "workerID", cm.workerID)
			return nil
		}
		return err
	}

	for _, f := range stateFiles {
		clientID := f.Name()
		data, err := os.ReadFile(cm.statePath(clientID))
		if err != nil {
			continue
		}

		var stateWithBatches map[string]interface{}
		if err := json.Unmarshal(data, &stateWithBatches); err != nil {
			if err := cm.processor.LoadClientState(clientID, data); err != nil {
				slog.Warn("CheckpointManager LoadState: failed to load client state", "clientID", clientID, "error", err)
				return err
			}
		} else {
			if stateData, ok := stateWithBatches["state"].(string); ok {
				if err := cm.processor.LoadClientState(clientID, []byte(stateData)); err != nil {
					slog.Warn("CheckpointManager LoadState: failed to load client state", "clientID", clientID, "error", err)
					return err
				}
			}
			if batchesData, ok := stateWithBatches["batches"].(map[string]interface{}); ok {
				batches := make(map[string]bool)
				for k, v := range batchesData {
					if b, ok := v.(bool); ok {
						batches[k] = b
					}
				}
				cm.mu.Lock()
				cm.processedBatches[clientID] = batches
				cm.mu.Unlock()
				slog.Debug("CheckpointManager LoadState: restored processed batches", "clientID", clientID, "batchCount", len(batches))
			}
		}

		cm.mu.Lock()
		cm.batchCount[clientID] = 0
		cm.pendingAcks[clientID] = nil
		cm.mu.Unlock()

		slog.Debug("CheckpointManager LoadState: restored client state from disk", "clientID", clientID, "workerName", cm.workerName, "workerID", cm.workerID)
	}

	slog.Debug("CheckpointManager LoadState finished", "workerName", cm.workerName, "workerID", cm.workerID, "clientsLoaded", len(stateFiles))
	return nil
}

func (cm *CheckpointManager) HasSeenBatch(clientID, batchID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	batches, ok := cm.processedBatches[clientID]
	if !ok {
		return false
	}
	return batches[batchID]
}

func (cm *CheckpointManager) RecordState(clientID string, batchID string) {
	cm.mu.Lock()
	cm.batchCount[clientID]++
	if cm.processedBatches[clientID] == nil {
		cm.processedBatches[clientID] = make(map[string]bool)
	}
	cm.processedBatches[clientID][batchID] = true
	slog.Debug("CheckpointManager RecordState", "clientID", clientID, "batchID", batchID, "batchCount", cm.batchCount[clientID])
	cm.mu.Unlock()
}

func (cm *CheckpointManager) ShouldFlush(clientID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	should := cm.batchCount[clientID] >= cm.checkpointEveryBatches
	slog.Debug("CheckpointManager ShouldFlush", "clientID", clientID, "batchCount", cm.batchCount[clientID], "shouldFlush", should)
	return should
}

func (cm *CheckpointManager) PersistAndAck(clientID string) error {
	cm.mu.Lock()
	if cm.batchCount[clientID] == 0 {
		cm.mu.Unlock()
		return nil
	}

	processedBatchesCopy := make(map[string]bool)
	for k, v := range cm.processedBatches[clientID] {
		processedBatchesCopy[k] = v
	}

	acksCopy := make([]func(), len(cm.pendingAcks[clientID]))
	copy(acksCopy, cm.pendingAcks[clientID])

	cm.mu.Unlock()

	state, err := cm.processor.GetClientState(clientID)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cm.stateDir(), 0755); err != nil {
		return err
	}

	path := cm.statePath(clientID)

	stateWithBatches := map[string]interface{}{
		"state":   string(state),
		"batches": processedBatchesCopy,
	}

	data, err := json.Marshal(stateWithBatches)
	if err != nil {
		return err
	}

	if err := os.WriteFile(path, data, 0644); err != nil {
		return err
	}

	slog.Debug("CheckpointManager PersistAndAck: checkpoint saved", "clientID", clientID, "path", path, "stateSize", len(data), "processedBatches", len(processedBatchesCopy))

	cm.mu.Lock()
	cm.pendingAcks[clientID] = nil
	cm.batchCount[clientID] = 0
	cm.mu.Unlock()

	for _, ack := range acksCopy {
		ack()
	}

	slog.Debug("CheckpointManager PersistAndAck: acks sent", "clientID", clientID, "ackCount", len(acksCopy))
	return nil
}

func (cm *CheckpointManager) AddPendingAck(clientID string, ack func()) {
	cm.mu.Lock()
	cm.pendingAcks[clientID] = append(cm.pendingAcks[clientID], ack)
	cm.mu.Unlock()
}

func (cm *CheckpointManager) HasPendingBatches(clientID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.batchCount[clientID] > 0
}

func (cm *CheckpointManager) ClearState(clientID string) error {
	if err := cm.processor.ClearClientState(clientID); err != nil {
		return err
	}

	cm.mu.Lock()
	delete(cm.pendingAcks, clientID)
	delete(cm.batchCount, clientID)
	delete(cm.processedBatches, clientID)
	cm.mu.Unlock()

	path := cm.statePath(clientID)
	if err := os.Remove(path); err != nil {
		if os.IsNotExist(err) {
			slog.Debug("CheckpointManager ClearState: state file did not exist", "clientID", clientID, "path", path)
			return nil
		}
		return err
	}

	slog.Debug("CheckpointManager ClearState: cleared checkpoint from disk", "clientID", clientID, "path", path)
	return nil
}

func (cm *CheckpointManager) stateDir() string {
	return fmt.Sprintf("/storage/%s-%d/state", cm.workerName, cm.workerID)
}

func (cm *CheckpointManager) statePath(clientID string) string {
	return fmt.Sprintf("%s/%s.json", cm.stateDir(), clientID)
}

func MarshalState(v any) ([]byte, error) {
	return json.Marshal(v)
}

func UnmarshalState(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
