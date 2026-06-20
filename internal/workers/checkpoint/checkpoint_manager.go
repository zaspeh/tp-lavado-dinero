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

func (cm *CheckpointManager) BeginBatch(clientID, batchID string, ack func()) (shouldProcess bool, err error) {
	cm.mu.Lock()
	if cm.processedBatches[clientID] != nil && cm.processedBatches[clientID][batchID] {
		cm.mu.Unlock()
		slog.Debug("CheckpointManager BeginBatch: already processed, acking", "clientID", clientID, "batchID", batchID)
		ack()
		return false, nil
	}

	cm.pendingAcks[clientID] = append(cm.pendingAcks[clientID], ack)
	slog.Debug("CheckpointManager BeginBatch: registered ack, waiting for CommitBatch", "clientID", clientID, "batchID", batchID)
	cm.mu.Unlock()

	return true, nil
}

// TODO: Situaciones de error y como reaccionar a estos.
func (cm *CheckpointManager) CommitBatch(clientID, batchID string) error {
	cm.mu.Lock()
	if cm.processedBatches[clientID] == nil {
		cm.processedBatches[clientID] = make(map[string]bool)
	}
	cm.processedBatches[clientID][batchID] = true
	cm.batchCount[clientID]++
	slog.Debug("CheckpointManager CommitBatch", "clientID", clientID, "batchID", batchID, "batchCount", cm.batchCount[clientID])

	shouldCheckpoint := cm.batchCount[clientID] >= cm.checkpointEveryBatches
	cm.mu.Unlock()

	if shouldCheckpoint {
		slog.Debug("CheckpointManager CommitBatch: triggering checkpoint", "clientID", clientID, "batchCount", cm.batchCount[clientID])
		return cm.persistAndAck(clientID)
	}

	return nil
}

func (cm *CheckpointManager) BeforeEOF(clientID string) error {
	cm.mu.Lock()
	hasPending := cm.batchCount[clientID] > 0
	cm.mu.Unlock()

	if !hasPending {
		return nil
	}

	slog.Debug("CheckpointManager BeforeEOF: persisting pending batches", "clientID", clientID)
	return cm.persistAndAck(clientID)
}

func (cm *CheckpointManager) persistAndAck(clientID string) error {
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

	if err := cm.atomicWriteFile(path, data); err != nil {
		return err
	}

	slog.Debug("CheckpointManager persistAndAck: checkpoint saved", "clientID", clientID, "path", path, "stateSize", len(data), "processedBatches", len(processedBatchesCopy))

	cm.mu.Lock()
	cm.pendingAcks[clientID] = nil
	cm.batchCount[clientID] = 0
	cm.mu.Unlock()

	for _, ack := range acksCopy {
		ack()
	}

	slog.Debug("CheckpointManager persistAndAck: acks sent", "clientID", clientID, "ackCount", len(acksCopy))
	return nil
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

func (cm *CheckpointManager) atomicWriteFile(path string, data []byte) error {
	tmpPath := path + ".tmp"

	f, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return err
	}

	if err := f.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return err
	}

	return nil
}

func MarshalState(v any) ([]byte, error) {
	return json.Marshal(v)
}

func UnmarshalState(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
