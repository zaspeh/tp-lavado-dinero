package coordinator

import (
	"log/slog"
)

type clientState struct {
	ownBatches  map[string]BatchRecord         // batches que fueron procesados por el nodo
	peerBatches map[int]map[string]BatchRecord // batches del resto de los peers, no locales

	// Estados eof
	seenEOFs      map[string]uint64
	eofCount      uint32
	expectedTotal uint64
	lastEOFID     string

	flushed bool
}

func newClientState() *clientState {
	return &clientState{
		ownBatches:  make(map[string]BatchRecord),
		peerBatches: make(map[int]map[string]BatchRecord),
		seenEOFs:    make(map[string]uint64),
	}
}

func (cs *clientState) hasOwnBatch(batchID string) bool {
	_, ok := cs.ownBatches[batchID]
	return ok
}

func (cs *clientState) addOwnBatch(record BatchRecord) {
	cs.ownBatches[record.BatchID] = record
}

func (cs *clientState) addPeerBatch(peerID int, record BatchRecord) {
	if cs.peerBatches[peerID] == nil {
		cs.peerBatches[peerID] = make(map[string]BatchRecord)
	}
	cs.peerBatches[peerID][record.BatchID] = record
}

func (cs *clientState) hasSeenEOF(eofID string) bool {
	_, ok := cs.seenEOFs[eofID]
	return ok
}

func (cs *clientState) markEOFSeen(eofID string, expectedTotal uint64) {
	cs.seenEOFs[eofID] = expectedTotal
	cs.expectedTotal += expectedTotal
	cs.eofCount++
	cs.lastEOFID = eofID
}

// totals calcula processed y survivors sobre la unión de batchIDs.
// Aqui es donde se hace la deduplicación de batches entre peers.
func (cs *clientState) totals() (uint64, uint64) {
	var processed, survivors uint64

	// Set global de batchIDs ya contados para deduplicar
	counted := make(map[string]bool)

	for batchID, record := range cs.ownBatches {
		counted[batchID] = true
		processed += record.Processed
		survivors += record.Survivors
	}

	for _, peerBatches := range cs.peerBatches {
		for batchID, record := range peerBatches {
			if counted[batchID] {
				continue // batch ya fue tomado en cuenta
			}
			counted[batchID] = true
			processed += record.Processed
			survivors += record.Survivors
		}
	}

	return processed, survivors
}

func (cs *clientState) isReadyToFlush(expectedEOFs uint32) bool {
	if cs.flushed {
		return false
	}
	if !cs.hasAllEOFs(expectedEOFs) {
		slog.Debug("Not ready to flush: not all EOFs", "eofCount", cs.eofCount, "expectedEOFs", expectedEOFs)
		return false
	}
	processed, _ := cs.totals()
	slog.Debug("isReadyToFlush check", "processed", processed, "expectedTotal", cs.expectedTotal)
	return processed >= cs.expectedTotal
}

func (cs *clientState) hasAllEOFs(expectedEOFs uint32) bool {
	return cs.eofCount >= expectedEOFs
}

// TODO: metodo parche no deberia ser necesario suarlo
func (cs *clientState) wouldHaveAllEOFs(expectedEOFs uint32) bool {
	return cs.eofCount+1 >= expectedEOFs
}
