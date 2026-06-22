package checkpoint

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
)

type checkpointFile struct {
	Entities         map[string][]byte `json:"entities"`
	ProcessedBatches map[string]bool   `json:"batches"`
}

type CheckpointManager struct {
	workerName             string
	workerID               int
	processor              Checkpointable
	checkpointEveryBatches int

	batchCount       map[string]int
	processedBatches map[string]map[string]bool
	dirtyEntities    map[string]map[string]bool
	pendingAcks      map[string][]func()
}

type CheckpointManagerConfig struct {
	WorkerName             string
	WorkerID               int
	Processor              Checkpointable
	CheckpointEveryBatches int
}

func NewCheckpointManager(checkpointConfig *CheckpointManagerConfig) *CheckpointManager {
	return &CheckpointManager{
		workerName:             checkpointConfig.WorkerName,
		workerID:               checkpointConfig.WorkerID,
		processor:              checkpointConfig.Processor,
		checkpointEveryBatches: checkpointConfig.CheckpointEveryBatches,
		batchCount:             make(map[string]int),
		processedBatches:       make(map[string]map[string]bool),
		dirtyEntities:          make(map[string]map[string]bool),
		pendingAcks:            make(map[string][]func()),
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

		var file checkpointFile
		if err := json.Unmarshal(data, &file); err != nil {
			slog.Warn("CheckpointManager LoadState: failed to parse checkpoint file", "clientID", clientID, "error", err)
			continue
		}

		for entityID, entityData := range file.Entities {
			if err := cm.processor.LoadEntity(clientID, entityID, entityData); err != nil {
				slog.Warn("CheckpointManager LoadState: failed to load entity", "clientID", clientID, "entityID", entityID, "error", err)
				return err
			}
		}

		batches := make(map[string]bool)
		for k, v := range file.ProcessedBatches {
			batches[k] = v
		}
		cm.processedBatches[clientID] = batches

		cm.batchCount[clientID] = 0
		cm.pendingAcks[clientID] = nil
		cm.dirtyEntities[clientID] = nil

		slog.Debug("CheckpointManager LoadState: restored client state from disk", "clientID", clientID, "entities", len(file.Entities), "batches", len(batches))
	}

	slog.Debug("CheckpointManager LoadState finished", "workerName", cm.workerName, "workerID", cm.workerID, "clientsLoaded", len(stateFiles))
	return nil
}

// BeginBatch registra un batch como pendiente de procesamiento. El ack
// se acumula y se dispara cuando CommitBatch persiste el estado a disco.
// Si el batch ya fue procesado (cargado de LoadState), el ack se dispara
// inmediatamente porque no hay nada que esperar.
func (cm *CheckpointManager) BeginBatch(clientID, batchID string, ack func()) (shouldProcess bool, err error) {
	if cm.processedBatches[clientID] != nil && cm.processedBatches[clientID][batchID] {
		slog.Debug("CheckpointManager BeginBatch: already processed, acking", "clientID", clientID, "batchID", batchID)
		ack()
		return false, nil
	}

	cm.pendingAcks[clientID] = append(cm.pendingAcks[clientID], ack)
	slog.Debug("CheckpointManager BeginBatch: registered ack, waiting for CommitBatch", "clientID", clientID, "batchID", batchID)

	return true, nil
}

func (cm *CheckpointManager) NotifyEntityChanged(clientID, entityID string) error {
	if cm.dirtyEntities[clientID] == nil {
		cm.dirtyEntities[clientID] = make(map[string]bool)
	}
	cm.dirtyEntities[clientID][entityID] = true
	slog.Debug("CheckpointManager NotifyEntityChanged", "clientID", clientID, "entityID", entityID)
	return nil
}

// AckEOF dispara el ack de un mensaje EOF inmediatamente.
// Los EOFs no se persisten como batches, así que el ack se puede hacer al
// terminar el procesamiento (después de BeforeEOF que persiste los pendientes).
func (cm *CheckpointManager) AckEOF(ack func()) {
	ack()
}

// AckCleanup dispara el ack de un mensaje Cleanup inmediatamente.
// Los Cleanups no se persisten como batches, así que el ack se puede hacer
// al terminar el procesamiento.
func (cm *CheckpointManager) AckCleanup(ack func()) {
	ack()
}

func (cm *CheckpointManager) CommitBatch(clientID, batchID string) error {
	if cm.processedBatches[clientID] == nil {
		cm.processedBatches[clientID] = make(map[string]bool)
	}
	cm.processedBatches[clientID][batchID] = true
	cm.batchCount[clientID]++
	slog.Debug("CheckpointManager CommitBatch", "clientID", clientID, "batchID", batchID, "batchCount", cm.batchCount[clientID])

	if cm.batchCount[clientID] >= cm.checkpointEveryBatches {
		slog.Debug("CheckpointManager CommitBatch: triggering checkpoint", "clientID", clientID, "batchCount", cm.batchCount[clientID])
		return cm.persistAndAck(clientID)
	}

	// Acks acumulados se disparan al final del procesamiento, no en cada commit.
	// El disparo real ocurre en BeforeEOF, persistAndAck o cuando se vacía el batchCount.
	return nil
}

func (cm *CheckpointManager) BeforeEOF(clientID string) error {
	if cm.batchCount[clientID] == 0 {
		return nil
	}

	slog.Debug("CheckpointManager BeforeEOF: persisting pending batches", "clientID", clientID)
	return cm.persistAndAck(clientID)
}

func (cm *CheckpointManager) persistAndAck(clientID string) error {
	if cm.batchCount[clientID] == 0 {
		return nil
	}

	entities := make(map[string][]byte)
	for entityID := range cm.dirtyEntities[clientID] {
		data, err := cm.processor.SerializeEntity(clientID, entityID)
		if err != nil {
			slog.Error("CheckpointManager persistAndAck: SerializeEntity failed", "clientID", clientID, "entityID", entityID, "error", err)
			return err
		}
		entities[entityID] = data
	}

	batches := make(map[string]bool)
	for k, v := range cm.processedBatches[clientID] {
		batches[k] = v
	}

	file := checkpointFile{
		Entities:         entities,
		ProcessedBatches: batches,
	}

	data, err := json.Marshal(file)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cm.stateDir(), 0755); err != nil {
		return err
	}

	path := cm.statePath(clientID)
	if err := cm.atomicWriteFile(path, data); err != nil {
		return err
	}

	slog.Debug("CheckpointManager persistAndAck: checkpoint saved", "clientID", clientID, "path", path, "entities", len(entities), "batches", len(batches))

	cm.batchCount[clientID] = 0
	cm.dirtyEntities[clientID] = nil

	acks := cm.pendingAcks[clientID]
	cm.pendingAcks[clientID] = nil

	for _, ack := range acks {
		ack()
	}

	slog.Debug("CheckpointManager persistAndAck: acks fired", "clientID", clientID, "ackCount", len(acks))
	return nil
}

func (cm *CheckpointManager) ClearState(clientID string) error {
	if err := cm.processor.ClearClientState(clientID); err != nil {
		return err
	}

	delete(cm.batchCount, clientID)
	delete(cm.processedBatches, clientID)
	delete(cm.dirtyEntities, clientID)
	delete(cm.pendingAcks, clientID)

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
