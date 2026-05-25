package eofcoordinator

import (
	"fmt"
	"log/slog"
	"sync"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
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
	FlushHandler      FlushHandler
}

type peerCount struct {
	processed uint64
	survivors uint64
}

type clientState struct {
	localProcessed uint64
	localSurvivors uint64
	EOFSeen        bool
	expectedTotal  uint64
	peerCount      map[int]peerCount
}

type EOFCoordinator struct {
	workerID     int
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

	return &EOFCoordinator{
		workerID:     config.WorkerID,
		publishKeys:  publishKeys,
		exchange:     exchange,
		flushHandler: config.FlushHandler,
		clients:      make(map[string]*clientState),
	}, nil
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

	if !state.EOFSeen {
		return nil
	}

	if err := c.broadcastLocalCount(clientID, state); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) RecordSurvivor(clientID string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)
	state.localSurvivors++

	if !state.EOFSeen {
		return nil
	}

	if err := c.broadcastLocalCount(clientID, state); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) HandleLocalEOF(clientID string, expectedTotal uint64) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	state := c.getClientState(clientID)
	state.EOFSeen = true
	state.expectedTotal = expectedTotal

	if err := c.broadcastLocalCount(clientID, state); err != nil {
		return err
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) handleCoordinationMessage(msg m.Message, ack, nack func()) {
	progressMsg, err := serializer.DeserializeCoordinationMessage(msg)
	if err != nil {
		slog.Debug("Failed to deserialize EOF progress message", "error", err)
		nack()
		return
	}
	c.handleRemoteEOF(progressMsg)
}

func (c *EOFCoordinator) handleRemoteEOF(coordinationMsg *protobuf.EOFCoordination) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	clientID := coordinationMsg.GetClientId()
	state := c.getClientState(clientID)
	state.EOFSeen = true
	state.peerCount[int(coordinationMsg.GetSenderId())] = peerCount{
		processed: coordinationMsg.GetProcessedCount(),
		survivors: coordinationMsg.GetSurvivorCount(),
	}

	return c.tryFlush(clientID, state)
}

func (c *EOFCoordinator) broadcastLocalCount(clientID string, state *clientState) error {
	coordinationMsg := &protobuf.EOFCoordination{
		ClientId:       clientID,
		SenderId:       uint32(c.workerID),
		ProcessedCount: state.localProcessed,
		SurvivorCount:  state.localSurvivors,
		ExpectedTotal:  state.expectedTotal,
	}
	message, err := serializer.SerializeCoordinationMessage(coordinationMsg)
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

func (c *EOFCoordinator) tryFlush(clientID string, state *clientState) error {
	if !state.EOFSeen {
		return nil
	}

	totalProcessed := state.localProcessed
	totalSurvivors := state.localSurvivors

	for _, peer := range state.peerCount {
		totalProcessed += peer.processed
		totalSurvivors += peer.survivors
	}

	if totalProcessed < state.expectedTotal {
		return nil
	}

	return c.flushHandler(clientID, totalSurvivors)
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
