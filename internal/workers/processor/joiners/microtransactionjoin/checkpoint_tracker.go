package microtransactionjoin

import (
	"encoding/json"
	"fmt"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindMicrotransaction = "microtransaction"
)

type microtransactionCheckpointValue struct {
	Account   string  `json:"account"`
	ToAccount string  `json:"toAccount"`
	Amount    float64 `json:"amount"`
}

type MicrotransactionJoinCheckpointTracker struct {
	stores                  *map[string]*MicrotransactionStore
	dirtyKeys               map[string]bool
	lastCheckpointedIndex   map[string]int
	pendingCheckpointOffset map[string]int
}

func NewMicrotransactionJoinCheckpointTracker(stores *map[string]*MicrotransactionStore) *MicrotransactionJoinCheckpointTracker {
	return &MicrotransactionJoinCheckpointTracker{
		stores:                  stores,
		dirtyKeys:               make(map[string]bool),
		lastCheckpointedIndex:   make(map[string]int),
		pendingCheckpointOffset: make(map[string]int),
	}
}

func (t *MicrotransactionJoinCheckpointTracker) MarkResultAdded(clientID string) {
	t.dirtyKeys[clientID] = true
}

func (t *MicrotransactionJoinCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	if !t.dirtyKeys[clientID] {
		return nil, nil
	}

	store := (*t.stores)[clientID]
	if store == nil {
		return nil, nil
	}

	results := store.GetResults()
	from := t.lastCheckpointedIndex[clientID]
	if from >= len(results) {
		delete(t.dirtyKeys, clientID)
		return nil, nil
	}

	changes := make([]checkpoint.CheckpointChange, 0, len(results)-from)
	for i := from; i < len(results); i++ {
		result := results[i]
		value, err := json.Marshal(microtransactionCheckpointValue{
			Account:   result.GetAccount(),
			ToAccount: result.GetToAccount(),
			Amount:    result.GetAmount(),
		})
		if err != nil {
			return nil, err
		}
		changes = append(changes, checkpoint.CheckpointChange{
			Kind:  checkpointKindMicrotransaction,
			Key:   fmt.Sprintf("%s:%d", clientID, i),
			Value: json.RawMessage(value),
		})
	}

	t.pendingCheckpointOffset[clientID] = from
	t.lastCheckpointedIndex[clientID] = len(results)
	delete(t.dirtyKeys, clientID)

	return changes, nil
}

func (t *MicrotransactionJoinCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	if previousOffset, ok := t.pendingCheckpointOffset[clientID]; ok {
		t.lastCheckpointedIndex[clientID] = previousOffset
		delete(t.pendingCheckpointOffset, clientID)
		t.dirtyKeys[clientID] = true
	}
	return nil
}

func (t *MicrotransactionJoinCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	switch change.Kind {
	case checkpointKindMicrotransaction:
		var value microtransactionCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		store := (*t.stores)[clientID]
		if store == nil {
			store = newMicrotransactionStore()
			(*t.stores)[clientID] = store
		}
		store.Add(&protobuf.Microtransaction{
			Account:   value.Account,
			ToAccount: value.ToAccount,
			Amount:    value.Amount,
		})
		t.lastCheckpointedIndex[clientID]++
	default:
		return fmt.Errorf("unknown microtransactionjoin checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *MicrotransactionJoinCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyKeys, clientID)
	delete(t.lastCheckpointedIndex, clientID)
	delete(t.pendingCheckpointOffset, clientID)
}
