package coordinator

import (
	"fmt"
	"sync"
)

type MultiEOFCoordinator struct {
	flushHandler FlushHandler

	workerID     int
	expectedEOFs uint64

	mu       sync.Mutex
	received map[string]uint64
}

func NewMultiEOFCoordinator(
	workerID int,
	expectedEOFs uint64,
) *MultiEOFCoordinator {
	return &MultiEOFCoordinator{
		workerID:     workerID,
		expectedEOFs: expectedEOFs,
		received:     make(map[string]uint64),
	}
}

func (c *MultiEOFCoordinator) HandleLocalEOF(
	clientID string,
	count uint64,
	eofID string,
) error {

	c.mu.Lock()
	defer c.mu.Unlock()

	c.received[clientID]++

	if c.received[clientID] < c.expectedEOFs {
		return nil
	}

	delete(c.received, clientID)

	newEofID := fmt.Sprintf("%s-%d", eofID, c.workerID)

	return c.flushHandler(clientID, count, newEofID)
}

func (c *MultiEOFCoordinator) SetFlushHandler(handler FlushHandler) {
	c.flushHandler = handler
}

func (c *MultiEOFCoordinator) RecordProcessed(clientID string) error {
	return nil
}

func (c *MultiEOFCoordinator) RecordSurvivor(clientID string) error {
	return nil
}

func (c *MultiEOFCoordinator) RecordBatch(
	clientID,
	batchID string,
	processed,
	survivors uint64,
) error {
	return nil
}

func (c *MultiEOFCoordinator) HasSeenBatch(
	clientID,
	batchID string,
) bool {
	return false
}

func (c *MultiEOFCoordinator) IsLeader() bool {
	return true
}

func (c *MultiEOFCoordinator) Run() error {
	return nil
}

func (c *MultiEOFCoordinator) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()

	clear(c.received)

	return nil
}
