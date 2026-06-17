package coordinator

import (
	"fmt"
	"log/slog"
	"sync"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

const defaultExpectedEOFs = 1

type EOFCoordinatorConfig struct {
	PeersExchangeName string
	ConnSettings      m.ConnSettings
	WorkerID          int
	WorkerCount       int
	ExpectedEOFs      int
	FlushHandler      FlushHandler
}

type EOFCoordinator struct {
	workerID     int
	expectedEOFs uint32
	publishKeys  []string
	exchange     *m.ExchangeMiddleware
	storage      *BatchStorage
	flushHandler FlushHandler
	mu           sync.Mutex
	clients      map[string]*clientState
}

func NewEOFCoordinator(config EOFCoordinatorConfig) (*EOFCoordinator, error) {
	subscriptionKey := []string{fmt.Sprintf("%s.%d", config.PeersExchangeName, config.WorkerID)}
	exchange, err := m.CreateExchangeMiddleware(config.PeersExchangeName, subscriptionKey, config.ConnSettings)
	if err != nil {
		return nil, err
	}

	publishKeys := make([]string, 0, config.WorkerCount-1)
	for i := range config.WorkerCount {
		if i == config.WorkerID {
			continue
		}
		publishKeys = append(publishKeys, fmt.Sprintf("%s.%d", config.PeersExchangeName, i))
	}

	if config.ExpectedEOFs == 0 {
		config.ExpectedEOFs = defaultExpectedEOFs
	}

	storage, err := NewBatchStorage(config.PeersExchangeName, config.WorkerID)
	if err != nil {
		exchange.Close()
		return nil, err
	}

	coord := &EOFCoordinator{
		workerID:     config.WorkerID,
		expectedEOFs: uint32(config.ExpectedEOFs),
		publishKeys:  publishKeys,
		exchange:     exchange,
		storage:      storage,
		flushHandler: config.FlushHandler,
		clients:      make(map[string]*clientState),
	}

	if err := coord.restoreFromStorage(); err != nil {
		storage.Close()
		exchange.Close()
		return nil, err
	}

	return coord, nil
}

// restoreFromStorage reconstruye el estado en memoria desde disco.
// Se llama solo en init, antes de procesar cualquier mensaje.
func (c *EOFCoordinator) restoreFromStorage() error {
	batches, err := c.storage.LoadBatches()
	if err != nil {
		return err
	}
	eofs, err := c.storage.LoadEOFs()
	if err != nil {
		return err
	}

	for clientID, clientBatches := range batches {
		state := c.getClientState(clientID)
		for _, record := range clientBatches {
			state.addOwnBatch(record)
		}
	}

	for clientID, clientEOFs := range eofs {
		state := c.getClientState(clientID)
		for eofID := range clientEOFs {
			// expectedTotal se re-sincroniza cuando llega el EOF por red,
			// los EOFs propios persistidos solo marcan que ya los vimos
			state.seenEOFs[eofID] = true
		}
	}

	return nil
}

func (c *EOFCoordinator) SetFlushHandler(handler FlushHandler) {
	c.flushHandler = handler
}

func (c *EOFCoordinator) Run() error {
	c.exchange.StartConsuming(func(msg m.Message, ack, nack func()) {
		c.handleCoordinationMessage(msg, ack, nack)
	})
	return nil
}

// HasSeenBatch es consultado por el engine antes de contar un batch.
func (c *EOFCoordinator) HasSeenBatch(clientID, batchID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.getClientState(clientID)
	return state.hasOwnBatch(batchID)
}

// RecordBatch registra un batch propio. El payload del batch se comparte con
// peers solo cuando la barrera de EOF ya esta activa.
func (c *EOFCoordinator) RecordBatch(clientID, batchID string, processed, survivors uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)
	if state.hasOwnBatch(batchID) {
		return nil
	}

	record := BatchRecord{BatchID: batchID, Processed: processed, Survivors: survivors}
	shouldBroadcast := state.hasAllEOFs(c.expectedEOFs)

	if shouldBroadcast {
		if err := c.broadcastBatch(clientID, record); err != nil {
			return err
		}
	}
	if err := c.storage.WriteBatch(clientID, record); err != nil {
		return err
	}
	state.addOwnBatch(record)

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) HandleLocalEOF(clientID string, expectedTotal uint64, eofID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Info("Handling local EOF", "clientID", clientID, "expectedTotal", expectedTotal)
	state := c.getClientState(clientID)

	if state.hasSeenEOF(eofID) {
		slog.Debug("Ignoring duplicate local EOF", "clientID", clientID, "eofID", eofID)
		return nil
	}

	if err := c.broadcastEOF(clientID, eofID, expectedTotal); err != nil {
		return err
	}

	if state.wouldHaveAllEOFs(c.expectedEOFs) {
		if err := c.broadcastOwnBatches(clientID, state); err != nil {
			return err
		}
	}
	if err := c.storage.WriteEOF(clientID, eofID); err != nil {
		return err
	}

	state.markEOFSeen(eofID, expectedTotal)

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) IsLeader() bool {
	return c.workerID == 0
}

func (c *EOFCoordinator) handleCoordinationMessage(msg m.Message, ack, nack func()) {
	progressMsg, err := serializer.DeserializeCoordinationMessage(msg)
	if err != nil {
		slog.Debug("Failed to deserialize coordination message", "error", err)
		nack()
		return
	}

	var handleErr error
	if progressMsg.GetEofArrived() {
		handleErr = c.handleRemoteEOF(progressMsg)
	} else {
		handleErr = c.handleRemoteBatch(progressMsg)
	}

	if handleErr != nil {
		slog.Debug("Failed to handle coordination message", "error", handleErr)
		nack()
		return
	}
	ack()
}

func (c *EOFCoordinator) handleRemoteEOF(msg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := msg.GetClientId()
	eofID := msg.GetEofID()
	if eofID == "" {
		eofID = fmt.Sprintf("peer-%d-empty-eof", msg.GetSenderId())
	}
	state := c.getClientState(clientID)

	if state.hasSeenEOF(eofID) {
		slog.Debug("Ignoring duplicate remote EOF", "clientID", clientID, "eofID", eofID)
		return nil
	}

	if state.wouldHaveAllEOFs(c.expectedEOFs) {
		if err := c.broadcastOwnBatches(clientID, state); err != nil {
			return err
		}
	}

	state.markEOFSeen(eofID, msg.GetExpectedTotal())

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) handleRemoteBatch(msg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := msg.GetClientId()
	batchID := msg.GetBatchID()
	peerID := int(msg.GetSenderId())

	state := c.getClientState(clientID)

	// Si el batch ya es nuestro, el peer procesó un duplicado de rabbit.
	// Lo ignoramos: nuestro conteo ya es correcto.
	if state.hasOwnBatch(batchID) {
		return nil
	}

	record := BatchRecord{
		BatchID:   batchID,
		Processed: msg.GetProcessedCount(),
		Survivors: msg.GetSurvivorCount(),
	}
	state.addPeerBatch(peerID, record)

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) broadcastBatch(clientID string, record BatchRecord) error {
	msg := &protobuf.EOFCoordination{
		ClientId:       clientID,
		SenderId:       uint32(c.workerID),
		BatchID:        record.BatchID,
		ProcessedCount: record.Processed,
		SurvivorCount:  record.Survivors,
	}
	return c.broadcast(msg)
}

func (c *EOFCoordinator) broadcastEOF(clientID, eofID string, expectedTotal uint64) error {
	msg := &protobuf.EOFCoordination{
		ClientId:      clientID,
		SenderId:      uint32(c.workerID),
		EofArrived:    true,
		EofID:         eofID,
		ExpectedTotal: expectedTotal,
	}
	return c.broadcast(msg)
}

func (c *EOFCoordinator) broadcastOwnBatches(clientID string, state *clientState) error {
	for _, record := range state.ownBatches {
		if err := c.broadcastBatch(clientID, record); err != nil {
			return err
		}
	}
	return nil
}

func (c *EOFCoordinator) broadcast(msg *protobuf.EOFCoordination) error {
	message, err := serializer.SerializeCoordinationMessage(msg)
	if err != nil {
		return err
	}
	for _, key := range c.publishKeys {
		if err := c.exchange.SendWithKey(key, message); err != nil {
			return err
		}
	}
	return nil
}

func (c *EOFCoordinator) getClientState(clientID string) *clientState {
	state, ok := c.clients[clientID]
	if !ok {
		state = newClientState()
		c.clients[clientID] = state
	}
	return state
}

func (c *EOFCoordinator) tryFlush(clientID string, state *clientState) error {
	processed, totalSurvivors := state.totals()
	if !state.isReadyToFlush(c.expectedEOFs) {
		slog.Debug("Not ready to flush", "clientID", clientID, "eofCount", state.eofCount,
			"expectedEOFs", c.expectedEOFs, "processed", processed, "expectedTotal", state.expectedTotal)
		return nil
	}

	if err := c.flushHandler(clientID, totalSurvivors, state.lastEOFID); err != nil {
		return err
	}
	state.flushed = true
	return nil
}

func (c *EOFCoordinator) Close() error {
	if err := c.exchange.Close(); err != nil {
		c.storage.Close()
		return err
	}
	return c.storage.Close()
}
