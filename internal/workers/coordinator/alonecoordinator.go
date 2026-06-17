package coordinator

import "fmt"

// Implementacion tipo patron Null Object
// Usado en nodos con router antes que ellos en el flujo
type AloneCoordinator struct {
	flushHandler FlushHandler
	workerID     int
}

func NewAloneCoordinator(workerID int) *AloneCoordinator {
	return &AloneCoordinator{workerID: workerID}
}

func (c *AloneCoordinator) SetFlushHandler(handler FlushHandler) {
	c.flushHandler = handler
}

// No necesitamos trackear procesados si no hay coordinación de red
func (c *AloneCoordinator) RecordProcessed(clientID string) error {
	return nil
}

func (c *AloneCoordinator) RecordSurvivor(clientID string) error {
	return nil
}

func (c *AloneCoordinator) RecordBatch(clientID, batchID string, processed, survivors uint64) error {
	return nil
}

func (c *AloneCoordinator) HasSeenBatch(clientID, batchID string) bool {
	return false
}

func (c *AloneCoordinator) HandleLocalEOF(clientID string, count uint64, eofID string) error {
	newEofID := fmt.Sprintf("%s-%d", eofID, c.workerID)
	return c.flushHandler(clientID, count, newEofID)
}

// Siempre es el líder de su propia partición aislada
func (c *AloneCoordinator) IsLeader() bool {
	return true
}

// No hay coordinación de red, así que no hay nada que hacer en Run o Close
func (c *AloneCoordinator) Run() error {
	return nil
}
func (c *AloneCoordinator) Close() error {
	return nil
}
