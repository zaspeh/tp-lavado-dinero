package origindestination

import (
	"encoding/json"
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const checkpointKindRelation = "relation"

type relationCheckpointValue struct {
	BaseBank       int32  `json:"baseBank"`
	BaseAccount    string `json:"baseAccount"`
	RelatedBank    int32  `json:"relatedBank"`
	RelatedAccount string `json:"relatedAccount"`
}

type AccountStoreCheckpointTracker struct {
	storeForClient func(clientID string) *AccountStore
	dirtyRelations map[string]map[relationCheckpointValue]struct{}
}

func NewAccountStoreCheckpointTracker(storeForClient func(clientID string) *AccountStore) *AccountStoreCheckpointTracker {
	return &AccountStoreCheckpointTracker{
		storeForClient: storeForClient,
		dirtyRelations: make(map[string]map[relationCheckpointValue]struct{}),
	}
}

func (t *AccountStoreCheckpointTracker) MarkRelationAdded(clientID string, base Account, related Account) {
	if t.dirtyRelations[clientID] == nil {
		t.dirtyRelations[clientID] = make(map[relationCheckpointValue]struct{})
	}
	t.dirtyRelations[clientID][relationCheckpointValue{
		BaseBank:       base.Bank,
		BaseAccount:    base.Account,
		RelatedBank:    related.Bank,
		RelatedAccount: related.Account,
	}] = struct{}{}
}

func (t *AccountStoreCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	relations := t.dirtyRelations[clientID]
	if len(relations) == 0 {
		return nil, nil
	}

	changes := make([]checkpoint.CheckpointChange, 0, len(relations))
	for relation := range relations {
		value, err := json.Marshal(relation)
		if err != nil {
			return nil, err
		}
		changes = append(changes, checkpoint.CheckpointChange{
			Kind:  checkpointKindRelation,
			Key:   relationKey(relation),
			Value: json.RawMessage(value),
		})
	}

	delete(t.dirtyRelations, clientID)
	return changes, nil
}

func (t *AccountStoreCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		switch change.Kind {
		case checkpointKindRelation:
			var value relationCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			t.MarkRelationAdded(clientID, value.baseAccount(), value.relatedAccount())
		}
	}
	return nil
}

func (t *AccountStoreCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	switch change.Kind {
	case checkpointKindRelation:
		var value relationCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		t.storeForClient(clientID).Add(value.baseAccount(), value.relatedAccount())
	default:
		return fmt.Errorf("unknown origindestination checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *AccountStoreCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyRelations, clientID)
}

func (v relationCheckpointValue) baseAccount() Account {
	return Account{Bank: v.BaseBank, Account: v.BaseAccount}
}

func (v relationCheckpointValue) relatedAccount() Account {
	return Account{Bank: v.RelatedBank, Account: v.RelatedAccount}
}

func relationKey(v relationCheckpointValue) string {
	return fmt.Sprintf("%d:%s:%d:%s", v.BaseBank, v.BaseAccount, v.RelatedBank, v.RelatedAccount)
}
