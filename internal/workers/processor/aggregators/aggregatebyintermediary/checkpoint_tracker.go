package aggregatebyintermediary

import (
	"encoding/json"
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindOrigin      = "origin"
	checkpointKindDestination = "destination"
)

type intermediaryRelationCheckpointValue struct {
	IntermediaryBank    int32  `json:"intermediaryBank"`
	IntermediaryAccount string `json:"intermediaryAccount"`
	AccountBank         int32  `json:"accountBank"`
	Account             string `json:"account"`
}

type AggregateByIntermediaryCheckpointTracker struct {
	storeForClient func(clientID string) *IntermediaryStore
	dirtyOrigins   map[string]map[intermediaryRelationCheckpointValue]struct{}
	dirtyDests     map[string]map[intermediaryRelationCheckpointValue]struct{}
}

func NewAggregateByIntermediaryCheckpointTracker(storeForClient func(clientID string) *IntermediaryStore) *AggregateByIntermediaryCheckpointTracker {
	return &AggregateByIntermediaryCheckpointTracker{
		storeForClient: storeForClient,
		dirtyOrigins:   make(map[string]map[intermediaryRelationCheckpointValue]struct{}),
		dirtyDests:     make(map[string]map[intermediaryRelationCheckpointValue]struct{}),
	}
}

func (t *AggregateByIntermediaryCheckpointTracker) MarkOriginAdded(clientID string, intermediary model.Account, origin model.Account) {
	if t.dirtyOrigins[clientID] == nil {
		t.dirtyOrigins[clientID] = make(map[intermediaryRelationCheckpointValue]struct{})
	}
	t.dirtyOrigins[clientID][newIntermediaryRelationValue(intermediary, origin)] = struct{}{}
}

func (t *AggregateByIntermediaryCheckpointTracker) MarkDestinationAdded(clientID string, intermediary model.Account, destination model.Account) {
	if t.dirtyDests[clientID] == nil {
		t.dirtyDests[clientID] = make(map[intermediaryRelationCheckpointValue]struct{})
	}
	t.dirtyDests[clientID][newIntermediaryRelationValue(intermediary, destination)] = struct{}{}
}

func (t *AggregateByIntermediaryCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	changes := make([]checkpoint.CheckpointChange, 0, len(t.dirtyOrigins[clientID])+len(t.dirtyDests[clientID]))

	for relation := range t.dirtyOrigins[clientID] {
		change, err := relationChange(checkpointKindOrigin, relation)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}

	for relation := range t.dirtyDests[clientID] {
		change, err := relationChange(checkpointKindDestination, relation)
		if err != nil {
			return nil, err
		}
		changes = append(changes, change)
	}

	delete(t.dirtyOrigins, clientID)
	delete(t.dirtyDests, clientID)
	return changes, nil
}

func (t *AggregateByIntermediaryCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		var value intermediaryRelationCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}

		switch change.Kind {
		case checkpointKindOrigin:
			t.MarkOriginAdded(clientID, value.intermediary(), value.account())
		case checkpointKindDestination:
			t.MarkDestinationAdded(clientID, value.intermediary(), value.account())
		}
	}
	return nil
}

func (t *AggregateByIntermediaryCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	var value intermediaryRelationCheckpointValue
	if err := json.Unmarshal(change.Value, &value); err != nil {
		return err
	}

	store := t.storeForClient(clientID)
	switch change.Kind {
	case checkpointKindOrigin:
		store.AddOrigin(value.intermediary(), value.account())
	case checkpointKindDestination:
		store.AddDestination(value.intermediary(), value.account())
	default:
		return fmt.Errorf("unknown aggregatebyintermediary checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *AggregateByIntermediaryCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyOrigins, clientID)
	delete(t.dirtyDests, clientID)
}

func relationChange(kind string, value intermediaryRelationCheckpointValue) (checkpoint.CheckpointChange, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return checkpoint.CheckpointChange{}, err
	}
	return checkpoint.CheckpointChange{
		Kind:  kind,
		Key:   fmt.Sprintf("%d:%s:%d:%s", value.IntermediaryBank, value.IntermediaryAccount, value.AccountBank, value.Account),
		Value: json.RawMessage(raw),
	}, nil
}

func newIntermediaryRelationValue(intermediary model.Account, account model.Account) intermediaryRelationCheckpointValue {
	return intermediaryRelationCheckpointValue{
		IntermediaryBank:    intermediary.Bank,
		IntermediaryAccount: intermediary.Account,
		AccountBank:         account.Bank,
		Account:             account.Account,
	}
}

func (v intermediaryRelationCheckpointValue) intermediary() model.Account {
	return model.Account{Bank: v.IntermediaryBank, Account: v.IntermediaryAccount}
}

func (v intermediaryRelationCheckpointValue) account() model.Account {
	return model.Account{Bank: v.AccountBank, Account: v.Account}
}
