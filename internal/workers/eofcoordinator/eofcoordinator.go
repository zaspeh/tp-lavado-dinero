package eofcoordinator

import (
	"fmt"
	"log/slog"
	"sync"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

const (
	defaultExpectedEOFs = 1
)

// Funcion del estilo callback usada para que una vez que los
// nodos hermanos hayan recibido la totalidad de los mensajes
// se haga flush correspondiente
type FlushHandler func(clientID string, survivorCount uint64) error

type EOFCoordinatorConfig struct {
	PeersExchangeName string
	ConnSettings      m.ConnSettings
	WorkerID          int
	WorkerCount       int
	ExpectedEOFs      int
	FlushHandler      FlushHandler
}

type peerCount struct {
	processed uint64
	survivors uint64
	eofSeen   uint32
}

type clientState struct {
	localProcessed uint64
	localSurvivors uint64
	eofSeenLocal   uint32
	expectedTotal  uint64
	peerCount      map[int]peerCount
	flushed        bool
}

type EOFCoordinator struct {
	workerID     int
	expectedEOFs uint32
	publishKeys  []string
	exchange     *m.ExchangeMiddleware
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
		key := fmt.Sprintf("%s.%d", config.PeersExchangeName, i)
		publishKeys = append(publishKeys, key)
	}

	if config.ExpectedEOFs == 0 {
		config.ExpectedEOFs = defaultExpectedEOFs
	}

	return &EOFCoordinator{
		workerID:     config.WorkerID,
		expectedEOFs: uint32(config.ExpectedEOFs),
		publishKeys:  publishKeys,
		exchange:     exchange,
		flushHandler: config.FlushHandler,
		clients:      make(map[string]*clientState),
	}, nil
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

func (c *EOFCoordinator) RecordProcessed(clientID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)
	state.localProcessed++

	if state.eofSeenLocal < c.expectedEOFs {
		return nil
	}

	if err := c.broadcastEOFCoordination(clientID, state, false, 0); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) RecordSurvivor(clientID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)
	state.localSurvivors++

	if state.eofSeenLocal < c.expectedEOFs {
		return nil
	}

	if err := c.broadcastEOFCoordination(clientID, state, false, 0); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) HandleLocalEOF(clientID string, expectedTotal uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	slog.Info("Handling local EOF", "clientID", clientID, "expectedTotal", expectedTotal)
	state := c.getClientState(clientID)
	state.eofSeenLocal++
	state.expectedTotal += expectedTotal

	slog.Debug("Handling local EOF - current state", "clientID", clientID, "state", state)
	if err := c.broadcastEOFCoordination(clientID, state, true, expectedTotal); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

// Logica para que los workers se fijen si enviar o no
// el eofa los nodos siguientes en el flujo. a cambiar cuanda
// exista eleccion de lider
func (c *EOFCoordinator) IsLeader() bool {
	return c.workerID == 0
}

func (c *EOFCoordinator) handleCoordinationMessage(msg m.Message, ack, nack func()) {
	progressMsg, err := serializer.DeserializeCoordinationMessage(msg)
	if err != nil {
		slog.Debug("Failed to deserialize EOF progress message", "error", err)
		nack()
		return
	}
	slog.Debug("Received EOF progress message", "message", progressMsg, "from", progressMsg.GetSenderId())
	if progressMsg.GetEofArrived() {
		if err := c.handleRemoteEOF(progressMsg); err != nil {
			slog.Debug("Failed to handle remote EOF message", "error", err)
			nack()
			return
		}
		ack()
		return
	}

	if err := c.handlePeerCount(progressMsg); err != nil {
		slog.Debug("Failed to handle remote EOF progress message", "error", err)
		nack()
		return
	}

	ack()
}

func (c *EOFCoordinator) handleRemoteEOF(progressMsg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := progressMsg.GetClientId()
	state := c.getClientState(clientID)
	state.eofSeenLocal++
	state.expectedTotal += progressMsg.GetExpectedTotal()
	c.updateCount(progressMsg)

	// Comunicamos estado actual a los otros nodos para que actualicen conteo
	if err := c.broadcastEOFCoordination(clientID, state, false, 0); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) updateCount(coordinationMsg *protobuf.EOFCoordination) {
	clientID := coordinationMsg.GetClientId()
	state := c.getClientState(clientID)
	state.peerCount[int(coordinationMsg.GetSenderId())] = peerCount{
		processed: coordinationMsg.GetProcessedCount(),
		survivors: coordinationMsg.GetSurvivorCount(),
		eofSeen:   coordinationMsg.GetEofSeen(),
	}
}

func (c *EOFCoordinator) handlePeerCount(coordinationMsg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.updateCount(coordinationMsg)
	clientID := coordinationMsg.GetClientId()
	state := c.getClientState(clientID)
	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) broadcastEOFCoordination(clientID string, state *clientState, eofArrived bool, totalToAdd uint64) error {
	coordinationMsg := &protobuf.EOFCoordination{
		ClientId:       clientID,
		SenderId:       uint32(c.workerID),
		ProcessedCount: state.localProcessed,
		SurvivorCount:  state.localSurvivors,
		ExpectedTotal:  totalToAdd,
		EofArrived:     eofArrived,
		EofSeen:        state.eofSeenLocal,
	}
	message, err := serializer.SerializeCoordinationMessage(coordinationMsg)
	if err != nil {
		return err
	}
	for _, key := range c.publishKeys {
		slog.Debug("Broadcasting to key", "key", key)
		if err := c.exchange.SendWithKey(key, message); err != nil {
			return err
		}
	}
	return nil
}

func (c *EOFCoordinator) tryFlush(clientID string, state *clientState) error {
	slog.Debug("Trying FLush")
	if state.flushed {
		slog.Debug("Already flushed", "clientID", clientID)
		return nil
	}
	if state.eofSeenLocal < c.expectedEOFs {
		slog.Debug("Seen Local < expectedEofs", "seen", state.eofSeenLocal, "expected", c.expectedEOFs)
		return nil
	}

	totalProcessed := state.localProcessed
	totalSurvivors := state.localSurvivors

	for _, peer := range state.peerCount {
		totalProcessed += peer.processed
		totalSurvivors += peer.survivors
	}

	if totalProcessed < state.expectedTotal {
		slog.Debug("totalProcesed < expectedTotal", "processed", totalProcessed, "expected", state.expectedTotal)
		return nil
	}

	if err := c.flushHandler(clientID, totalSurvivors); err != nil {
		return err
	}
	state.flushed = true
	return nil
}

func (c *EOFCoordinator) getClientState(clientID string) *clientState {
	state, ok := c.clients[clientID]
	if ok {
		return state
	}

	state = &clientState{peerCount: make(map[int]peerCount)}
	c.clients[clientID] = state
	return state
}

func (c *EOFCoordinator) Close() error {
	return c.exchange.Close()
}
