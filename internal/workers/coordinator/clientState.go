package coordinator

import (
	"maps"
	"sort"
)

type clientState struct {
	ownBatches  map[string]BatchRecord         // batches que fueron procesados por el nodo
	peerBatches map[int]map[string]BatchRecord // batches del resto de los peers, no locales

	// Estados eof
	ownEOFs   map[string]uint64
	peerEOFs  map[int]map[string]uint64
	lastEOFID string

	flushed bool
}

func newClientState() *clientState {
	return &clientState{
		ownBatches:  make(map[string]BatchRecord),
		peerBatches: make(map[int]map[string]BatchRecord),
		ownEOFs:     make(map[string]uint64),
		peerEOFs:    make(map[int]map[string]uint64),
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

func (cs *clientState) hasOwnEOF(eofID string) bool {
	_, ok := cs.ownEOFs[eofID]
	return ok
}

func (cs *clientState) hasPeerEOF(peerID int, eofID string) bool {
	if cs.peerEOFs[peerID] == nil {
		return false
	}
	_, ok := cs.peerEOFs[peerID][eofID]
	return ok
}

func (cs *clientState) addOwnEOF(eofID string, expectedTotal uint64) {
	cs.ownEOFs[eofID] = expectedTotal

	cs.getLastEOFID()
}

func (cs *clientState) addPeerEOF(peerID int, eofID string, expectedTotal uint64) {
	if cs.peerEOFs[peerID] == nil {
		cs.peerEOFs[peerID] = make(map[string]uint64)
	}
	cs.peerEOFs[peerID][eofID] = expectedTotal
	cs.getLastEOFID()
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

func (cs *clientState) eofTotals() (uint32, uint64) {
	counted := make(map[string]uint64)

	maps.Copy(counted, cs.ownEOFs)

	for _, peerEOFs := range cs.peerEOFs {
		for eofID, expectedTotal := range peerEOFs {
			if _, exists := counted[eofID]; exists {
				continue
			}
			counted[eofID] = expectedTotal
		}
	}

	var expectedTotal uint64
	for _, total := range counted {
		expectedTotal += total
	}
	return uint32(len(counted)), expectedTotal
}

func (cs *clientState) isReadyToFlush(expectedEOFs uint32) bool {
	if cs.flushed {
		return false
	}
	if !cs.hasAllEOFs(expectedEOFs) {
		return false
	}
	processed, _ := cs.totals()
	_, expectedTotal := cs.eofTotals()
	return processed >= expectedTotal
}

func (cs *clientState) hasAllEOFs(expectedEOFs uint32) bool {
	eofCount, _ := cs.eofTotals()
	return eofCount >= expectedEOFs
}

func (cs *clientState) getLastEOFID() {
	allEOFIDs := make([]string, 0, len(cs.ownEOFs))

	for eofID := range cs.ownEOFs {
		allEOFIDs = append(allEOFIDs, eofID)
	}

	for _, peerEOFs := range cs.peerEOFs {
		for eofID := range maps.Keys(peerEOFs) {
			allEOFIDs = append(allEOFIDs, eofID)
		}
	}

	sort.Strings(allEOFIDs)
	cs.lastEOFID = allEOFIDs[len(allEOFIDs)-1]
}
