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

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
)

const legacyEntityChangeKind = "entity"

type CheckpointManager struct {
	workerName             string
	workerID               int
	checkpointEveryBatches int
	participants           []checkpointParticipant

	mu               sync.Mutex
	nextSeq          map[string]uint64
	batchCount       map[string]int
	processedBatches map[string]map[string]bool
	pendingBatches   map[string][]string
	dirtyEntities    map[string]map[string]bool
	pendingAcks      map[string][]func()
	eofSent          map[string]bool
	finalizeComplete map[string]bool
	processedCount   map[string]map[string]uint64
}

type CheckpointManagerConfig struct {
	WorkerName             string
	WorkerID               int
	Processor              Checkpointable
	Participants           []ChangeCheckpointable
	CheckpointEveryBatches int
}

type checkpointParticipant struct {
	name         string
	base         Checkpointable
	checkpointer ChangeCheckpointable
	entity       EntityCheckpointable
}

type drainedParticipantChanges struct {
	participant checkpointParticipant
	changes     []CheckpointChange
}

func NewCheckpointManager(checkpointConfig *CheckpointManagerConfig) *CheckpointManager {
	return &CheckpointManager{
		workerName:             checkpointConfig.WorkerName,
		workerID:               checkpointConfig.WorkerID,
		checkpointEveryBatches: checkpointConfig.CheckpointEveryBatches,
		participants:           normalizeParticipants(checkpointConfig),
		nextSeq:                make(map[string]uint64),
		batchCount:             make(map[string]int),
		processedBatches:       make(map[string]map[string]bool),
		pendingBatches:         make(map[string][]string),
		dirtyEntities:          make(map[string]map[string]bool),
		pendingAcks:            make(map[string][]func()),
		eofSent:                make(map[string]bool),
		finalizeComplete:       make(map[string]bool),
		processedCount:         make(map[string]map[string]uint64),
	}
}

func normalizeParticipants(config *CheckpointManagerConfig) []checkpointParticipant {
	if len(config.Participants) > 0 {
		participants := make([]checkpointParticipant, 0, len(config.Participants))
		for _, participant := range config.Participants {
			participants = append(participants, checkpointParticipant{
				name:         participantName(participant),
				base:         participant,
				checkpointer: participant,
			})
		}
		return participants
	}

	if config.Processor == nil {
		return nil
	}

	if changeProcessor, ok := config.Processor.(ChangeCheckpointable); ok {
		return []checkpointParticipant{{
			name:         participantName(changeProcessor),
			base:         changeProcessor,
			checkpointer: changeProcessor,
		}}
	}

	if entityProcessor, ok := config.Processor.(EntityCheckpointable); ok {
		return []checkpointParticipant{{
			name:   participantName(entityProcessor),
			base:   entityProcessor,
			entity: entityProcessor,
		}}
	}

	return []checkpointParticipant{{
		name: participantName(config.Processor),
		base: config.Processor,
	}}
}

func participantName(participant Checkpointable) string {
	named, ok := participant.(NamedCheckpointable)
	if !ok {
		return ""
	}
	return named.CheckpointParticipantName()
}

func (cm *CheckpointManager) SetCheckpointEveryBatches(n int) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	cm.checkpointEveryBatches = n
}

func (cm *CheckpointManager) LoadState(coordinator coordinator.Coordinator) error {
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
		if err := cm.loadClientState(clientID, coordinator); err != nil {
			return err
		}
	}

	slog.Debug("CheckpointManager LoadState finished", "workerName", cm.workerName, "workerID", cm.workerID, "clientsLoaded", len(clients))
	return nil
}

func (cm *CheckpointManager) loadClientState(clientID string, coordinator coordinator.Coordinator) error {
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
	// TODO: Escritura de changes del entry -> +16 mb
	scanner.Buffer(make([]byte, 64*1024), 128*1024*1024)
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

		for i, batchID := range entry.Batches {
			batches[batchID] = true
			err := coordinator.RecordBatch(clientID, batchID, entry.ProcessedCounts[i], 0)
			if err != nil {
				return err
			}
		}
		for _, change := range entry.Changes {
			if change.Kind == "eofSent" {
				cm.eofSent[clientID] = true
				slog.Debug("CheckpointManager LoadState: restored eofSent marker", "clientID", clientID)
				continue
			}
			if change.Kind == "finalizeComplete" {
				cm.finalizeComplete[clientID] = true
				slog.Debug("CheckpointManager LoadState: restored finalizeComplete marker", "clientID", clientID)
				continue
			}
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
		slog.Info("CheckpointManager BeginBatch: already processed, acking", "clientID", clientID, "batchID", batchID)
		ack()
		return false, nil
	}

	if cm.processedBatches[clientID] == nil {
		cm.processedBatches[clientID] = make(map[string]bool)
	}
	cm.processedBatches[clientID][batchID] = true

	cm.pendingBatches[clientID] = append(cm.pendingBatches[clientID], batchID)

	cm.pendingAcks[clientID] = append(cm.pendingAcks[clientID], ack)
	cm.batchCount[clientID]++
	slog.Debug("CheckpointManager BeginBatch: registered ack, waiting for CommitBatch", "clientID", clientID, "batchID", batchID)

	return true, nil
}

func (cm *CheckpointManager) AbortBatch(clientID string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	if cm.batchCount[clientID] > 0 {
		cm.batchCount[clientID]--
	}
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

func (cm *CheckpointManager) SetEofSent(clientID string) error {
	cm.mu.Lock()
	cm.eofSent[clientID] = true
	cm.mu.Unlock()

	slog.Debug("CheckpointManager SetEofSent: persisting EOF sent marker", "clientID", clientID)
	return cm.persistEofMarker(clientID)
}

func (cm *CheckpointManager) IsEofSent(clientID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	return cm.eofSent[clientID]
}

func (cm *CheckpointManager) NeedsFinalize(clientID string) bool {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	hasBatches := cm.processedBatches[clientID] != nil && len(cm.processedBatches[clientID]) > 0
	notFinalized := !cm.finalizeComplete[clientID]
	result := hasBatches && notFinalized
	slog.Debug("CheckpointManager NeedsFinalize", "clientID", clientID, "hasBatches", hasBatches, "notFinalized", notFinalized, "result", result)
	return result
}

func (cm *CheckpointManager) GetClientsNeedingFinalize() []string {
	cm.mu.Lock()
	defer cm.mu.Unlock()
	var clients []string
	for clientID := range cm.processedBatches {
		if len(cm.processedBatches[clientID]) > 0 && !cm.finalizeComplete[clientID] {
			clients = append(clients, clientID)
		}
	}
	slog.Debug("CheckpointManager GetClientsNeedingFinalize", "clients", clients)
	return clients
}

func (cm *CheckpointManager) SetFinalizeComplete(clientID string) {
	cm.mu.Lock()
	cm.finalizeComplete[clientID] = true
	cm.mu.Unlock()

	slog.Debug("CheckpointManager SetFinalizeComplete: persisting marker", "clientID", clientID)
	cm.persistFinalizeCompleteMarker(clientID)
}

func (cm *CheckpointManager) persistFinalizeCompleteMarker(clientID string) error {
	seq := cm.nextSeq[clientID]
	if seq == 0 {
		seq = 1
	}

	entry := CheckpointLogEntry{
		Seq:     seq,
		Batches: nil,
		Changes: []CheckpointChange{
			{
				Kind:  "finalizeComplete",
				Key:   clientID,
				Value: json.RawMessage(`{}`),
			},
		},
	}

	if err := cm.appendLogEntry(clientID, entry); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.nextSeq[clientID] = seq + 1
	cm.mu.Unlock()

	return nil
}

func (cm *CheckpointManager) persistEofMarker(clientID string) error {
	seq := cm.nextSeq[clientID]
	if seq == 0 {
		seq = 1
	}

	entry := CheckpointLogEntry{
		Seq:     seq,
		Batches: nil,
		Changes: []CheckpointChange{
			{
				Kind:  "eofSent",
				Key:   clientID,
				Value: json.RawMessage(`{}`),
			},
		},
	}

	if err := cm.appendLogEntry(clientID, entry); err != nil {
		return err
	}

	cm.mu.Lock()
	cm.nextSeq[clientID] = seq + 1
	cm.mu.Unlock()

	return nil
}

func (cm *CheckpointManager) CommitBatch(clientID, batchID string, processedCount uint64, coordinator coordinator.Coordinator) error {
	cm.mu.Lock()
	shouldPersist := cm.batchCount[clientID] >= cm.checkpointEveryBatches
	batchCount := cm.batchCount[clientID]

	if cm.processedCount[clientID] == nil {
		cm.processedCount[clientID] = make(map[string]uint64)
	}
	cm.processedCount[clientID][batchID] = processedCount
	cm.mu.Unlock()

	slog.Debug("CheckpointManager CommitBatch", "clientID", clientID, "batchID", batchID, "batchCount", batchCount)
	if shouldPersist || coordinator.ReachedEOFAmount(clientID) {
		slog.Debug("CheckpointManager CommitBatch: triggering checkpoint", "clientID", clientID, "batchCount", batchCount)
		return cm.persistAndAck(clientID, coordinator)

	}
	return nil
}

/*
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
*/
func (cm *CheckpointManager) FlushPendingBatches(coordinator coordinator.Coordinator, clientID string) error {
	if coordinator.ReachedEOFAmount(clientID) {
		slog.Debug("CheckpointManager FlushPendingBatches: triggering checkpoint", "clientID", clientID)
		return cm.persistAndAck(clientID, coordinator)
	}
	return nil

}

func (cm *CheckpointManager) persistAndAck(clientID string, coordinator coordinator.Coordinator) error {
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

	processedCount := make([]uint64, 0, len(cm.processedCount[clientID]))

	for _, count := range cm.processedCount[clientID] {
		processedCount = append(processedCount, count)

	}

	entry := CheckpointLogEntry{
		Seq:             seq,
		Batches:         batches,
		Changes:         changes,
		ProcessedCounts: processedCount,
	}
	if err := cm.appendLogEntry(clientID, entry); err != nil {
		cm.restoreChanges(clientID, changes)
		return err
	}

	for batchID, count := range cm.processedCount[clientID] {
		coordinator.RecordBatch(clientID, batchID, count, 0)
	}

	cm.mu.Lock()
	cm.nextSeq[clientID] = seq + 1
	cm.batchCount[clientID] = 0
	cm.pendingBatches[clientID] = nil
	cm.dirtyEntities[clientID] = nil
	acks := cm.pendingAcks[clientID]
	cm.pendingAcks[clientID] = nil
	cm.processedCount[clientID] = make(map[string]uint64)

	cm.mu.Unlock()

	for _, ack := range acks {
		ack()
	}

	slog.Debug("CheckpointManager persistAndAck: checkpoint log entry saved", "clientID", clientID, "seq", seq, "batches", len(batches), "changes", len(changes), "ackCount", len(acks))
	return nil
}

func (cm *CheckpointManager) ClearState(clientID string) error {
	for _, participant := range cm.participants {
		if err := participant.clearClientState(clientID); err != nil {
			return err
		}
	}

	cm.mu.Lock()
	delete(cm.nextSeq, clientID)
	delete(cm.batchCount, clientID)
	delete(cm.processedBatches, clientID)
	delete(cm.pendingBatches, clientID)
	delete(cm.dirtyEntities, clientID)
	delete(cm.pendingAcks, clientID)
	delete(cm.eofSent, clientID)
	delete(cm.finalizeComplete, clientID)
	cm.mu.Unlock()

	path := cm.clientStateDir(clientID)
	if err := os.RemoveAll(path); err != nil {
		return err
	}

	slog.Debug("CheckpointManager ClearState: cleared checkpoint from disk", "clientID", clientID, "path", path)
	return nil
}

func (cm *CheckpointManager) drainChanges(clientID string) ([]CheckpointChange, error) {
	changes, drained, err := cm.drainParticipantChanges(clientID)
	if err != nil {
		cm.restoreDrainedChanges(clientID, drained)
		return nil, err
	}
	return changes, nil
}

func (cm *CheckpointManager) restoreChanges(clientID string, changes []CheckpointChange) {
	drained := cm.groupChangesByParticipant(changes)
	cm.restoreDrainedChanges(clientID, drained)
}

func (cm *CheckpointManager) applyChange(clientID string, change CheckpointChange) error {
	participant, routedChange, err := cm.routeChange(change)
	if err != nil {
		return err
	}
	return participant.applyChange(clientID, routedChange)
}

func (cm *CheckpointManager) drainParticipantChanges(clientID string) ([]CheckpointChange, []drainedParticipantChanges, error) {
	if err := cm.validateParticipantRouting(); err != nil {
		return nil, nil, err
	}

	allChanges := make([]CheckpointChange, 0)
	drained := make([]drainedParticipantChanges, 0, len(cm.participants))

	for _, participant := range cm.participants {
		changes, err := cm.drainParticipant(clientID, participant)
		if err != nil {
			return nil, drained, err
		}
		if len(changes) == 0 {
			continue
		}

		drained = append(drained, drainedParticipantChanges{
			participant: participant,
			changes:     append([]CheckpointChange(nil), changes...),
		})

		for _, change := range changes {
			if participant.name == "" && len(cm.participants) > 1 && change.Kind != legacyEntityChangeKind {
				return nil, drained, fmt.Errorf("checkpoint participant emitted unnamespaced change kind %q with multiple participants", change.Kind)
			}
			allChanges = append(allChanges, participant.persistedChange(change))
		}
	}

	return allChanges, drained, nil
}

func (cm *CheckpointManager) validateParticipantRouting() error {
	seenNames := make(map[string]bool)
	for _, participant := range cm.participants {
		if participant.name == "" {
			continue
		}
		if seenNames[participant.name] {
			return fmt.Errorf("duplicate checkpoint participant name %q", participant.name)
		}
		seenNames[participant.name] = true
	}
	return nil
}

func (cm *CheckpointManager) drainParticipant(clientID string, participant checkpointParticipant) ([]CheckpointChange, error) {
	if participant.checkpointer != nil {
		return participant.checkpointer.DrainChanges(clientID)
	}

	if participant.entity == nil {
		return nil, fmt.Errorf("checkpoint participant does not support changes or entities")
	}

	cm.mu.Lock()
	dirty := make([]string, 0, len(cm.dirtyEntities[clientID]))
	for entityID := range cm.dirtyEntities[clientID] {
		dirty = append(dirty, entityID)
	}
	cm.mu.Unlock()

	changes := make([]CheckpointChange, 0, len(dirty))
	for _, entityID := range dirty {
		data, err := participant.entity.SerializeEntity(clientID, entityID)
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

func (cm *CheckpointManager) restoreDrainedChanges(clientID string, drained []drainedParticipantChanges) {
	for _, item := range drained {
		if err := item.participant.restoreChanges(clientID, item.changes); err != nil {
			slog.Warn("CheckpointManager persistAndAck: failed to restore drained changes", "clientID", clientID, "participant", item.participant.name, "error", err)
		}
	}
}

func (cm *CheckpointManager) groupChangesByParticipant(changes []CheckpointChange) []drainedParticipantChanges {
	grouped := make([]drainedParticipantChanges, 0)
	indexByParticipant := make(map[int]int)

	for _, change := range changes {
		participant, routedChange, err := cm.routeChange(change)
		if err != nil {
			slog.Warn("CheckpointManager persistAndAck: failed to route change for restore", "kind", change.Kind, "key", change.Key, "error", err)
			continue
		}
		participantIndex := cm.participantIndex(participant)
		groupIndex, ok := indexByParticipant[participantIndex]
		if !ok {
			groupIndex = len(grouped)
			indexByParticipant[participantIndex] = groupIndex
			grouped = append(grouped, drainedParticipantChanges{participant: participant})
		}
		grouped[groupIndex].changes = append(grouped[groupIndex].changes, routedChange)
	}

	return grouped
}

func (cm *CheckpointManager) participantIndex(participant checkpointParticipant) int {
	for i, current := range cm.participants {
		if current.sameParticipant(participant) {
			return i
		}
	}
	return -1
}

func (cm *CheckpointManager) routeChange(change CheckpointChange) (checkpointParticipant, CheckpointChange, error) {
	if change.Kind == legacyEntityChangeKind {
		for _, participant := range cm.participants {
			if participant.entity != nil {
				return participant, change, nil
			}
		}
		return checkpointParticipant{}, change, fmt.Errorf("checkpoint change kind %q has no entity participant", change.Kind)
	}

	if prefix, rest, ok := strings.Cut(change.Kind, "."); ok {
		var matched *checkpointParticipant
		for _, participant := range cm.participants {
			if participant.name == prefix {
				if matched != nil {
					return checkpointParticipant{}, change, fmt.Errorf("duplicate checkpoint participant name %q", prefix)
				}
				current := participant
				matched = &current
			}
		}
		if matched != nil {
			change.Kind = rest
			return *matched, change, nil
		}
	}

	unnamedIncremental := make([]checkpointParticipant, 0, 1)
	for _, participant := range cm.participants {
		if participant.checkpointer != nil && participant.name == "" {
			unnamedIncremental = append(unnamedIncremental, participant)
		}
	}
	if len(unnamedIncremental) == 1 {
		return unnamedIncremental[0], change, nil
	}

	if len(cm.participants) == 1 && cm.participants[0].checkpointer != nil {
		return cm.participants[0], change, nil
	}

	return checkpointParticipant{}, change, fmt.Errorf("ambiguous checkpoint change kind %q; use a participant namespace", change.Kind)
}

func (p checkpointParticipant) persistedChange(change CheckpointChange) CheckpointChange {
	if p.name == "" || change.Kind == legacyEntityChangeKind || strings.HasPrefix(change.Kind, p.name+".") {
		return change
	}
	change.Kind = p.name + "." + change.Kind
	return change
}

func (p checkpointParticipant) applyChange(clientID string, change CheckpointChange) error {
	if p.checkpointer != nil {
		return p.checkpointer.ApplyChange(clientID, change)
	}
	if p.entity != nil && change.Kind == legacyEntityChangeKind {
		return p.entity.LoadEntity(clientID, change.Key, change.Value)
	}
	return fmt.Errorf("checkpoint participant cannot apply change kind %q", change.Kind)
}

func (p checkpointParticipant) restoreChanges(clientID string, changes []CheckpointChange) error {
	if restorable, ok := p.changeOrEntity().(RestorableChangeCheckpointable); ok {
		return restorable.RestoreChanges(clientID, changes)
	}
	return nil
}

func (p checkpointParticipant) clearClientState(clientID string) error {
	checkpointable := p.changeOrEntity()
	if checkpointable == nil {
		return nil
	}
	return checkpointable.ClearClientState(clientID)
}

func (p checkpointParticipant) changeOrEntity() Checkpointable {
	if p.checkpointer != nil {
		return p.checkpointer
	}
	if p.entity != nil {
		return p.entity
	}
	return p.base
}

func (p checkpointParticipant) sameParticipant(other checkpointParticipant) bool {
	return p.name == other.name && p.changeOrEntity() == other.changeOrEntity()
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
