package checkpoint

import (
	"bufio"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

const legacyEntityChangeKind = "entity"

type CheckpointManager struct {
	workerName             string
	workerID               int
	processor              Checkpointable
	checkpointEveryBatches int

	mu               sync.Mutex
	nextSeq          map[string]uint64
	batchCount       map[string]int
	processedBatches map[string]map[string]bool
	pendingBatches   map[string][]string
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
		nextSeq:                make(map[string]uint64),
		batchCount:             make(map[string]int),
		processedBatches:       make(map[string]map[string]bool),
		pendingBatches:         make(map[string][]string),
		dirtyEntities:          make(map[string]map[string]bool),
		pendingAcks:            make(map[string][]func()),
	}
}

func (cm *CheckpointManager) SetCheckpointEveryBatches(n int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.checkpointEveryBatches = n
}

func (cm *CheckpointManager) LoadState() error {
	slog.Debug("CheckpointManager LoadState starting", "workerName", cm.workerName, "workerID", cm.workerID)

	if err := os.MkdirAll(cm.stateDir(), 0755); err != nil {
		return err
	}

	clients, err := os.ReadDir(cm.stateDir())
	if err != nil {
		return err
	}

	for _, clientEntry := range clients {
		if !clientEntry.IsDir() {
			continue
		}

		clientID := clientEntry.Name()
		if err := cm.loadClientState(clientID); err != nil {
			return err
		}
	}

	slog.Debug("CheckpointManager LoadState finished", "workerName", cm.workerName, "workerID", cm.workerID, "clientsLoaded", len(clients))
	return nil
}

func (cm *CheckpointManager) loadClientState(clientID string) error {
	// Snapshot support is intentionally left for a future iteration.
	// Expected path: <client-state-dir>/snapshot.json

	logPath := cm.logPath(clientID)
	f, err := os.Open(logPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	batches := make(map[string]bool)
	var lastSeq uint64
	validEntries := 0

	scanner := bufio.NewScanner(f)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lineNumber := 0
	for scanner.Scan() {
		lineNumber++
		entry, ok, err := parseLogLine(scanner.Text())
		if err != nil {
			return err
		}
		if !ok {
			slog.Warn("CheckpointManager LoadState: corrupt checkpoint log line, stopping replay", "clientID", clientID, "line", lineNumber)
			break
		}

		for _, batchID := range entry.Batches {
			batches[batchID] = true
		}
		for _, change := range entry.Changes {
			if err := cm.applyChange(clientID, change); err != nil {
				slog.Warn("CheckpointManager LoadState: failed to apply checkpoint change", "clientID", clientID, "kind", change.Kind, "key", change.Key, "error", err)
				return err
			}
		}

		lastSeq = entry.Seq
		validEntries++
	}
	if err := scanner.Err(); err != nil {
		return err
	}

	cm.processedBatches[clientID] = batches
	cm.nextSeq[clientID] = lastSeq + 1
	cm.batchCount[clientID] = 0
	cm.pendingBatches[clientID] = nil
	cm.pendingAcks[clientID] = nil
	cm.dirtyEntities[clientID] = nil

	slog.Debug("CheckpointManager LoadState: restored client state from disk", "clientID", clientID, "entries", validEntries, "batches", len(batches), "nextSeq", lastSeq+1)
	return nil
}

// BeginBatch registra un batch como pendiente de procesamiento. El ack
// se acumula y se dispara luego de persistir el checkpoint correspondiente.
// Si el batch ya fue procesado durante recovery, se ackea inmediatamente.
func (cm *CheckpointManager) BeginBatch(clientID, batchID string, ack func()) (shouldProcess bool, err error) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

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
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if cm.dirtyEntities[clientID] == nil {
		cm.dirtyEntities[clientID] = make(map[string]bool)
	}
	cm.dirtyEntities[clientID][entityID] = true
	slog.Debug("CheckpointManager NotifyEntityChanged", "clientID", clientID, "entityID", entityID)
	return nil
}

func (cm *CheckpointManager) AckEOF(ack func()) {
	ack()
}

func (cm *CheckpointManager) AckCleanup(ack func()) {
	ack()
}

func (cm *CheckpointManager) CommitBatch(clientID, batchID string) error {
	cm.mu.Lock()
	if cm.processedBatches[clientID] == nil {
		cm.processedBatches[clientID] = make(map[string]bool)
	}
	cm.processedBatches[clientID][batchID] = true
	cm.pendingBatches[clientID] = append(cm.pendingBatches[clientID], batchID)
	cm.batchCount[clientID]++
	shouldPersist := cm.batchCount[clientID] >= cm.checkpointEveryBatches
	batchCount := cm.batchCount[clientID]
	cm.mu.Unlock()

	slog.Debug("CheckpointManager CommitBatch", "clientID", clientID, "batchID", batchID, "batchCount", batchCount)
	if shouldPersist {
		slog.Debug("CheckpointManager CommitBatch: triggering checkpoint", "clientID", clientID, "batchCount", batchCount)
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

	seq := cm.nextSeq[clientID]
	batches := append([]string(nil), cm.pendingBatches[clientID]...)
	cm.mu.Unlock()

	changes, err := cm.drainChanges(clientID)
	if err != nil {
		return err
	}

	entry := CheckpointLogEntry{
		Seq:     seq,
		Batches: batches,
		Changes: changes,
	}
	if err := cm.appendLogEntry(clientID, entry); err != nil {
		cm.restoreChanges(clientID, changes)
		return err
	}

	cm.mu.Lock()
	cm.nextSeq[clientID] = seq + 1
	cm.batchCount[clientID] = 0
	cm.pendingBatches[clientID] = nil
	cm.dirtyEntities[clientID] = nil
	acks := cm.pendingAcks[clientID]
	cm.pendingAcks[clientID] = nil
	cm.mu.Unlock()

	for _, ack := range acks {
		ack()
	}

	slog.Debug("CheckpointManager persistAndAck: checkpoint log entry saved", "clientID", clientID, "seq", seq, "batches", len(batches), "changes", len(changes), "ackCount", len(acks))
	return nil
}

func (cm *CheckpointManager) ClearState(clientID string) error {
	if err := cm.processor.ClearClientState(clientID); err != nil {
		return err
	}

	cm.mu.Lock()
	delete(cm.nextSeq, clientID)
	delete(cm.batchCount, clientID)
	delete(cm.processedBatches, clientID)
	delete(cm.pendingBatches, clientID)
	delete(cm.dirtyEntities, clientID)
	delete(cm.pendingAcks, clientID)
	cm.mu.Unlock()

	path := cm.clientStateDir(clientID)
	if err := os.RemoveAll(path); err != nil {
		return err
	}

	slog.Debug("CheckpointManager ClearState: cleared checkpoint from disk", "clientID", clientID, "path", path)
	return nil
}

func (cm *CheckpointManager) drainChanges(clientID string) ([]CheckpointChange, error) {
	if changeProcessor, ok := cm.processor.(ChangeCheckpointable); ok {
		return changeProcessor.DrainChanges(clientID)
	}

	entityProcessor, ok := cm.processor.(EntityCheckpointable)
	if !ok {
		return nil, fmt.Errorf("checkpoint processor does not support changes or entities")
	}

	cm.mu.Lock()
	dirty := make([]string, 0, len(cm.dirtyEntities[clientID]))
	for entityID := range cm.dirtyEntities[clientID] {
		dirty = append(dirty, entityID)
	}
	cm.mu.Unlock()

	changes := make([]CheckpointChange, 0, len(dirty))
	for _, entityID := range dirty {
		data, err := entityProcessor.SerializeEntity(clientID, entityID)
		if err != nil {
			slog.Error("CheckpointManager persistAndAck: SerializeEntity failed", "clientID", clientID, "entityID", entityID, "error", err)
			return nil, err
		}
		changes = append(changes, CheckpointChange{
			Kind:  legacyEntityChangeKind,
			Key:   entityID,
			Value: json.RawMessage(data),
		})
	}

	return changes, nil
}

func (cm *CheckpointManager) restoreChanges(clientID string, changes []CheckpointChange) {
	if restorable, ok := cm.processor.(RestorableChangeCheckpointable); ok {
		if err := restorable.RestoreChanges(clientID, changes); err != nil {
			slog.Warn("CheckpointManager persistAndAck: failed to restore drained changes", "clientID", clientID, "error", err)
		}
	}
}

func (cm *CheckpointManager) applyChange(clientID string, change CheckpointChange) error {
	if changeProcessor, ok := cm.processor.(ChangeCheckpointable); ok && change.Kind != legacyEntityChangeKind {
		return changeProcessor.ApplyChange(clientID, change)
	}

	entityProcessor, ok := cm.processor.(EntityCheckpointable)
	if !ok {
		return fmt.Errorf("checkpoint processor cannot apply change kind %q", change.Kind)
	}
	if change.Kind != legacyEntityChangeKind {
		return fmt.Errorf("unsupported checkpoint change kind %q", change.Kind)
	}
	return entityProcessor.LoadEntity(clientID, change.Key, change.Value)
}

func (cm *CheckpointManager) appendLogEntry(clientID string, entry CheckpointLogEntry) error {
	data, err := json.Marshal(entry)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(cm.clientStateDir(clientID), 0755); err != nil {
		return err
	}

	line := fmt.Appendf(nil, "%d %08x ", len(data), crc32.ChecksumIEEE(data))
	line = append(line, data...)
	line = append(line, '\n')

	f, err := os.OpenFile(cm.logPath(clientID), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.Write(line); err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}

func parseLogLine(line string) (CheckpointLogEntry, bool, error) {
	var entry CheckpointLogEntry

	firstSpace := strings.IndexByte(line, ' ')
	if firstSpace <= 0 {
		return entry, false, nil
	}
	secondSpace := strings.IndexByte(line[firstSpace+1:], ' ')
	if secondSpace <= 0 {
		return entry, false, nil
	}
	secondSpace += firstSpace + 1

	length, err := strconv.Atoi(line[:firstSpace])
	if err != nil || length < 0 {
		return entry, false, nil
	}

	expectedCRC, err := strconv.ParseUint(line[firstSpace+1:secondSpace], 16, 32)
	if err != nil {
		return entry, false, nil
	}

	jsonData := []byte(line[secondSpace+1:])
	if len(jsonData) != length {
		return entry, false, nil
	}
	if uint32(expectedCRC) != crc32.ChecksumIEEE(jsonData) {
		return entry, false, nil
	}
	if err := json.Unmarshal(jsonData, &entry); err != nil {
		return entry, false, nil
	}

	return entry, true, nil
}

func (cm *CheckpointManager) stateDir() string {
	return fmt.Sprintf("/storage/%s-%d/state", cm.workerName, cm.workerID)
}

func (cm *CheckpointManager) clientStateDir(clientID string) string {
	return filepath.Join(cm.stateDir(), clientID)
}

func (cm *CheckpointManager) logPath(clientID string) string {
	return filepath.Join(cm.clientStateDir(clientID), "changes.log")
}

func MarshalState(v any) ([]byte, error) {
	return json.Marshal(v)
}

func UnmarshalState(data []byte, v any) error {
	return json.Unmarshal(data, v)
}
