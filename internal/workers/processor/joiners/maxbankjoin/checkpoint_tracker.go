package maxbankjoin

import (
	"encoding/json"
	"fmt"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindResult = "result"
)

type resultCheckpointValue struct {
	BankName string `json:"bankName"`
	Account  string `json:"account"`
	Amount   string `json:"amount"`
}

type MaxBankJoinCheckpointTracker struct {
	stores    *map[string]*MaxBankStore
	dirtyKeys map[string]bool
}

func NewMaxBankJoinCheckpointTracker(stores *map[string]*MaxBankStore) *MaxBankJoinCheckpointTracker {
	return &MaxBankJoinCheckpointTracker{
		stores:    stores,
		dirtyKeys: make(map[string]bool),
	}
}

func (t *MaxBankJoinCheckpointTracker) MarkResultAdded(clientID string) {
	t.dirtyKeys[clientID] = true
}

func (t *MaxBankJoinCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	if !t.dirtyKeys[clientID] {
		return nil, nil
	}

	store := (*t.stores)[clientID]
	if store == nil {
		return nil, nil
	}

	results := store.GetResults()
	changes := make([]checkpoint.CheckpointChange, 0, len(results))

	for i, result := range results {
		value, err := json.Marshal(resultCheckpointValue{
			BankName: result.GetBankName(),
			Account:  result.GetAccount(),
			Amount:   result.GetAmount(),
		})
		if err != nil {
			return nil, err
		}
		changes = append(changes, checkpoint.CheckpointChange{
			Kind:  checkpointKindResult,
			Key:   fmt.Sprintf("%s:%d", clientID, i),
			Value: json.RawMessage(value),
		})
	}

	delete(t.dirtyKeys, clientID)
	return changes, nil
}

func (t *MaxBankJoinCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		switch change.Kind {
		case checkpointKindResult:
			var value resultCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			store := (*t.stores)[clientID]
			if store == nil {
				store = newMaxBankStore()
				(*t.stores)[clientID] = store
			}
			store.Add(&protobuf.MaxBankResult{
				BankName: value.BankName,
				Account:  value.Account,
				Amount:   value.Amount,
			})
			t.MarkResultAdded(clientID)
		}
	}
	return nil
}

func (t *MaxBankJoinCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	switch change.Kind {
	case checkpointKindResult:
		var value resultCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		store := (*t.stores)[clientID]
		if store == nil {
			store = newMaxBankStore()
			(*t.stores)[clientID] = store
		}
		store.Add(&protobuf.MaxBankResult{
			BankName: value.BankName,
			Account:  value.Account,
			Amount:   value.Amount,
		})
	default:
		return fmt.Errorf("unknown maxbankjoin checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *MaxBankJoinCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyKeys, clientID)
}