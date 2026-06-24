package coordinator

import (
	"fmt"
	"log/slog"
	"strconv"
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
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
	MaxBatchWeight    int
}

type EOFCoordinator struct {
	workerID         int
	workerName       string
	expectedEOFs     uint32
	publishKeys      []string
	exchange         *m.ExchangeMiddleware
	storage          *BatchStorage
	flushHandler     FlushHandler
	mu               sync.Mutex
	clients          map[string]*clientState
	innerBatchWeight int
	cleanedClients   map[string]bool
}

func NewEOFCoordinator(config EOFCoordinatorConfig) (*EOFCoordinator, error) {
	subscriptionKey := []string{fmt.Sprintf("%s.%d", config.PeersExchangeName, config.WorkerID)}
	exchange, err := m.CreateExchangeMiddleware(config.PeersExchangeName, subscriptionKey, config.ConnSettings, true, true, strconv.Itoa(config.WorkerID), "")
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
	if err := exchange.SetUp(); err != nil {
		exchange.Close()
		return nil, err
	}

	coord := &EOFCoordinator{
		workerID:         config.WorkerID,
		workerName:       config.PeersExchangeName,
		expectedEOFs:     uint32(config.ExpectedEOFs),
		publishKeys:      publishKeys,
		exchange:         exchange,
		storage:          storage,
		flushHandler:     config.FlushHandler,
		clients:          make(map[string]*clientState),
		innerBatchWeight: config.MaxBatchWeight,
		cleanedClients:   make(map[string]bool),
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
		for eofID, expectedTotal := range clientEOFs {
			// El valor se completa cuando un peer reenvía el EOF al despertar.
			state.markEOFSeen(eofID, expectedTotal)
		}
	}

	return nil
}

func (c *EOFCoordinator) SetFlushHandler(handler FlushHandler) {
	c.flushHandler = handler
}

func (c *EOFCoordinator) Run() error {
	if err := c.wakeUpNotification(); err != nil {
		return err
	}

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
		toSend := c.transformBatchToProto(record)
		if err := c.broadcastBatch([]*protobuf.BatchInformation{toSend}, clientID); err != nil {
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

	slog.Info("Handling local EOF", "clientID", clientID, "expectedTotal", expectedTotal, "eofID", eofID)
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
	if err := c.storage.WriteEOF(clientID, eofID, expectedTotal); err != nil {
		return err
	}

	state.markEOFSeen(eofID, expectedTotal)

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) IsLeader() bool {
	return c.workerID == 0
}

func (c *EOFCoordinator) wakeUpNotification() error {
	slog.Info("Sending wake up notification")
	msg := &protobuf.EOFCoordination{
		Type:     protobuf.CoordinationMessageType_wakeup,
		SenderId: uint32(c.workerID),
	}
	return c.broadcast(msg)
}

func (c *EOFCoordinator) handleCoordinationMessage(msg m.Message, ack, nack func()) {
	progressMsg, err := serializer.DeserializeCoordinationMessage(msg)
	if err != nil {
		slog.Debug("Failed to deserialize coordination message", "error", err)
		nack()
		return
	}

	var handleErr error
	switch progressMsg.GetType() {
	case protobuf.CoordinationMessageType_wakeup:
		handleErr = c.handleWakeUp(progressMsg)
	case protobuf.CoordinationMessageType_eof_arrived:
		handleErr = c.handleRemoteEOF(progressMsg)
	case protobuf.CoordinationMessageType_batch_information:
		handleErr = c.handleRemoteBatch(progressMsg)
	case protobuf.CoordinationMessageType_cleanup:
		handleErr = c.handleRemoteCleanup(progressMsg)
	}

	if handleErr != nil {
		slog.Debug("Failed to handle coordination message", "error", handleErr)
		nack()
		return
	}
	ack()
}

func (c *EOFCoordinator) handleWakeUp(msg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Info("Handling wake up message from", "senderID", msg.GetSenderId())
	key := fmt.Sprintf("%s.%d", c.workerName, msg.GetSenderId())

	for clientID, state := range c.clients {
		for eofID, expectedTotal := range state.seenEOFs {
			if err := c.sendEOF(key, clientID, eofID, expectedTotal); err != nil {
				return err
			}
		}

		// En caso de ue el nodo aun no tenga todos los EOFs,
		// no se inicio protocolo de broadcast, no damos informacion
		if !state.hasAllEOFs(c.expectedEOFs) {
			continue
		}

		batchOfInformation := batch.New(c.innerBatchWeight, protowrappers.ProtoSizer[*protobuf.BatchInformation](), protowrappers.FalseWrap)
		onflush := func(items []*protobuf.BatchInformation, batchID string) error {
			return c.responseToWakeUp(items, clientID, key)
		}

		batcher := batch.NewBatcher(batchOfInformation, onflush)
		for _, record := range state.ownBatches {
			toSend := c.transformBatchToProto(record)
			if err := batcher.Add(toSend); err != nil {
				return err
			}
		}

		if err := batcher.Flush(); err != nil {
			return err
		}
	}

	return nil
}

func (c *EOFCoordinator) responseToWakeUp(items []*protobuf.BatchInformation, clientID, key string) error {
	protoMsg := &protobuf.EOFCoordination{
		Type:        protobuf.CoordinationMessageType_batch_information,
		ClientId:    clientID,
		SenderId:    uint32(c.workerID),
		Information: items,
	}

	message, err := serializer.SerializeCoordinationMessage(protoMsg)
	if err != nil {
		return err
	}

	if err := c.exchange.SendWithKey(key, message); err != nil {
		return err
	}
	return nil
}

func (c *EOFCoordinator) handleRemoteEOF(msg *protobuf.EOFCoordination) error {
	slog.Info("handleRemoteEOF received", "clientID", msg.GetClientId(), "eofID", msg.GetEofID(), "senderID", msg.GetSenderId())

	c.mu.Lock()
	defer c.mu.Unlock()
	slog.Info("Handling remote EOF", "clientID", msg.GetClientId(), "eofID", msg.GetEofID(), "expectedTotal", msg.GetExpectedTotal())

	clientID := msg.GetClientId()
	eofID := msg.GetEofID()
	state := c.getClientState(clientID)
	if c.cleanedClients[clientID] {
		return nil
	}

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
	if c.cleanedClients[clientID] {
		return nil
	}
	peerID := int(msg.GetSenderId())

	state := c.getClientState(clientID)

	for _, batchInformation := range msg.GetInformation() {
		// Si el batch ya es nuestro, el peer procesó un duplicado de rabbit.
		// Lo ignoramos: nuestro conteo ya es correcto.
		if state.hasOwnBatch(batchInformation.GetBatchID()) {
			continue
		}

		record := BatchRecord{
			BatchID:   batchInformation.GetBatchID(),
			Processed: batchInformation.GetProcessedCount(),
			Survivors: batchInformation.GetSurvivorCount(),
		}
		state.addPeerBatch(peerID, record)
	}
	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) broadcastBatch(record []*protobuf.BatchInformation, clientID string) error {
	msg := &protobuf.EOFCoordination{
		Type:        protobuf.CoordinationMessageType_batch_information,
		ClientId:    clientID,
		SenderId:    uint32(c.workerID),
		Information: record,
	}
	return c.broadcast(msg)
}

func (c *EOFCoordinator) broadcastEOF(clientID, eofID string, expectedTotal uint64) error {
	slog.Info("broadcastEOF called", "clientID", clientID, "eofID", eofID, "expectedTotal", expectedTotal)
	msg := &protobuf.EOFCoordination{
		Type:          protobuf.CoordinationMessageType_eof_arrived,
		ClientId:      clientID,
		SenderId:      uint32(c.workerID),
		EofID:         eofID,
		ExpectedTotal: expectedTotal,
	}
	return c.broadcast(msg)
}

func (c *EOFCoordinator) sendEOF(key, clientID, eofID string, expectedTotal uint64) error {
	msg := &protobuf.EOFCoordination{
		Type:          protobuf.CoordinationMessageType_eof_arrived,
		ClientId:      clientID,
		SenderId:      uint32(c.workerID),
		EofID:         eofID,
		ExpectedTotal: expectedTotal,
	}
	message, err := serializer.SerializeCoordinationMessage(msg)
	if err != nil {
		return err
	}
	return c.exchange.SendWithKey(key, message)
}

func (c *EOFCoordinator) broadcastOwnBatches(clientID string, state *clientState) error {
	batchOfInformation := batch.New(c.innerBatchWeight, protowrappers.ProtoSizer[*protobuf.BatchInformation](), protowrappers.FalseWrap)
	onflush := func(items []*protobuf.BatchInformation, batchID string) error {
		return c.broadcastBatch(items, clientID)
	}

	batcher := batch.NewBatcher(batchOfInformation, onflush)
	for _, record := range state.ownBatches {
		toSend := c.transformBatchToProto(record)
		if err := batcher.Add(toSend); err != nil {
			return err
		}
	}

	return batcher.Flush()
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
		slog.Info("Not ready to flush", "clientID", clientID, "eofCount", state.eofCount,
			"expectedEOFs", c.expectedEOFs, "processed", processed, "expectedTotal", state.expectedTotal)
		return nil
	}
	slog.Info("READY TO flush", "clientID", clientID, "eofCount", state.eofCount,
		"expectedEOFs", c.expectedEOFs, "processed", processed, "expectedTotal", state.expectedTotal)

	if err := c.flushHandler(clientID, totalSurvivors, state.lastEOFID); err != nil {
		return err
	}
	state.flushed = true
	return nil
}

func (c *EOFCoordinator) ReachedEOFAmount(clientID string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)

	return state.hasAllEOFs(c.expectedEOFs)
}

func (c *EOFCoordinator) Close() error {
	if err := c.exchange.Close(); err != nil {
		c.storage.Close()
		return err
	}
	return c.storage.Close()
}

func (c *EOFCoordinator) ClearClient(clientID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cleanedClients[clientID] = true
	delete(c.clients, clientID)
	if err := c.storage.ClearClient(clientID); err != nil {
		slog.Error("EOFCoordinator: failed to clear client storage", "error", err, "clientID", clientID)
		return err
	}
	slog.Info("EOFCoordinator: cleared client state", "clientID", clientID)
	return nil
}

func (c *EOFCoordinator) BroadcastCleanup(clientID string) error {
	coordMsg := &protobuf.EOFCoordination{
		Type:     protobuf.CoordinationMessageType_cleanup,
		ClientId: clientID,
		SenderId: uint32(c.workerID),
	}

	if err := c.broadcast(coordMsg); err != nil {
		slog.Error("EOFCoordinator: failed to broadcast cleanup", "error", err, "clientID", clientID)
		return err
	}

	slog.Info("EOFCoordinator: broadcasted cleanup to peers", "clientID", clientID)
	return nil
}

func (c *EOFCoordinator) handleRemoteCleanup(msg *protobuf.EOFCoordination) error {
	clientID := msg.GetClientId()
	if c.cleanedClients[clientID] {
		return nil
	}
	c.ClearClient(clientID)
	slog.Info("EOFCoordinator: handled remote cleanup", "clientID", clientID, "senderID", msg.GetSenderId())
	return nil
}

func (c *EOFCoordinator) transformBatchToProto(batch BatchRecord) *protobuf.BatchInformation {
	return &protobuf.BatchInformation{
		BatchID:        batch.BatchID,
		ProcessedCount: batch.Processed,
		SurvivorCount:  batch.Survivors,
	}
}
