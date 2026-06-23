package avgbytypejoin

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
	Account    string `json:"account"`
	AmountPaid string `json:"amountPaid"`
}

type AvgByTypeJoinCheckpointTracker struct {
	storeForClient func(clientID string) *AvgByTypeResultStore
	dirtyResults   map[string]map[string]bool
}

func NewAvgByTypeJoinCheckpointTracker(storeForClient func(clientID string) *AvgByTypeResultStore) *AvgByTypeJoinCheckpointTracker {
	return &AvgByTypeJoinCheckpointTracker{
		storeForClient: storeForClient,
		dirtyResults:   make(map[string]map[string]bool),
	}
}

func (t *AvgByTypeJoinCheckpointTracker) MarkResultAdded(clientID, account string) {
	if t.dirtyResults[clientID] == nil {
		t.dirtyResults[clientID] = make(map[string]bool)
	}
	t.dirtyResults[clientID][account] = true
}

func (t *AvgByTypeJoinCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	store := t.storeForClient(clientID)
	changes := make([]checkpoint.CheckpointChange, 0)

	if accounts, ok := t.dirtyResults[clientID]; ok {
		results := store.GetResults()
		for _, result := range results {
			if accounts[result.GetAccount()] {
				value, err := json.Marshal(resultCheckpointValue{
					Account:    result.GetAccount(),
					AmountPaid: result.GetAmountPaid(),
				})
				if err != nil {
					return nil, err
				}
				changes = append(changes, checkpoint.CheckpointChange{
					Kind:  checkpointKindResult,
					Key:   result.GetAccount(),
					Value: json.RawMessage(value),
				})
			}
		}
	}

	delete(t.dirtyResults, clientID)
	return changes, nil
}

func (t *AvgByTypeJoinCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	store := t.storeForClient(clientID)

	for _, change := range changes {
		switch change.Kind {
		case checkpointKindResult:
			var value resultCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			store.Add(&protobuf.AvgByTypeResult{
				Account:    value.Account,
				AmountPaid: value.AmountPaid,
			})
			t.MarkResultAdded(clientID, value.Account)
		}
	}
	return nil
}

func (t *AvgByTypeJoinCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	store := t.storeForClient(clientID)

	switch change.Kind {
	case checkpointKindResult:
		var value resultCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		store.Add(&protobuf.AvgByTypeResult{
			Account:    value.Account,
			AmountPaid: value.AmountPaid,
		})
	default:
		return fmt.Errorf("unknown avgbytypejoin checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *AvgByTypeJoinCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyResults, clientID)
}