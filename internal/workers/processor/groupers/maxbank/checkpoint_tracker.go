package maxbank

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindBankName = "bankName"
	checkpointKindMaxTx    = "maxTx"
)

type bankNameCheckpointValue struct {
	BankID   int32  `json:"bankId"`
	BankName string `json:"bankName"`
}

type maxTxCheckpointValue struct {
	BankID       int32   `json:"bankId"`
	Account      string  `json:"account"`
	AmountValue  float64 `json:"amountValue"`
	AmountString string  `json:"amountString"`
}

type MaxBankCheckpointTracker struct {
	storeForClient func(clientID string) *MaxBankStore
	dirtyBankNames map[string]map[int32]string
	dirtyMaxTx     map[string]map[int32]Record
}

func NewMaxBankCheckpointTracker(storeForClient func(clientID string) *MaxBankStore) *MaxBankCheckpointTracker {
	return &MaxBankCheckpointTracker{
		storeForClient: storeForClient,
		dirtyBankNames: make(map[string]map[int32]string),
		dirtyMaxTx:     make(map[string]map[int32]Record),
	}
}

func (t *MaxBankCheckpointTracker) MarkBankName(clientID string, bankID int32, bankName string) {
	if t.dirtyBankNames[clientID] == nil {
		t.dirtyBankNames[clientID] = make(map[int32]string)
	}
	t.dirtyBankNames[clientID][bankID] = bankName
}

func (t *MaxBankCheckpointTracker) MarkMaxTransaction(clientID string, bankID int32, record Record) {
	if t.dirtyMaxTx[clientID] == nil {
		t.dirtyMaxTx[clientID] = make(map[int32]Record)
	}
	t.dirtyMaxTx[clientID][bankID] = record
}

func (t *MaxBankCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	changes := make([]checkpoint.CheckpointChange, 0, len(t.dirtyBankNames[clientID])+len(t.dirtyMaxTx[clientID]))

	bankNameIDs := make([]int, 0, len(t.dirtyBankNames[clientID]))
	for bankID := range t.dirtyBankNames[clientID] {
		bankNameIDs = append(bankNameIDs, int(bankID))
	}

	for _, bankIDInt := range bankNameIDs {
		bankID := int32(bankIDInt)
		value, err := json.Marshal(bankNameCheckpointValue{
			BankID:   bankID,
			BankName: t.dirtyBankNames[clientID][bankID],
		})
		if err != nil {
			return nil, err
		}
		changes = append(changes, checkpoint.CheckpointChange{
			Kind:  checkpointKindBankName,
			Key:   strconv.FormatInt(int64(bankID), 10),
			Value: json.RawMessage(value),
		})
	}

	maxTxIDs := make([]int, 0, len(t.dirtyMaxTx[clientID]))
	for bankID := range t.dirtyMaxTx[clientID] {
		maxTxIDs = append(maxTxIDs, int(bankID))
	}

	for _, bankIDInt := range maxTxIDs {
		bankID := int32(bankIDInt)
		record := t.dirtyMaxTx[clientID][bankID]
		value, err := json.Marshal(maxTxCheckpointValue{
			BankID:       bankID,
			Account:      record.Account,
			AmountValue:  record.AmountValue,
			AmountString: record.AmountString,
		})
		if err != nil {
			return nil, err
		}
		changes = append(changes, checkpoint.CheckpointChange{
			Kind:  checkpointKindMaxTx,
			Key:   strconv.FormatInt(int64(bankID), 10),
			Value: json.RawMessage(value),
		})
	}

	delete(t.dirtyBankNames, clientID)
	delete(t.dirtyMaxTx, clientID)
	return changes, nil
}

func (t *MaxBankCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		switch change.Kind {
		case checkpointKindBankName:
			var value bankNameCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			t.MarkBankName(clientID, value.BankID, value.BankName)
		case checkpointKindMaxTx:
			var value maxTxCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			t.MarkMaxTransaction(clientID, value.BankID, Record{
				Account:      value.Account,
				AmountValue:  value.AmountValue,
				AmountString: value.AmountString,
			})
		}
	}
	return nil
}

func (t *MaxBankCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	store := t.storeForClient(clientID)

	switch change.Kind {
	case checkpointKindBankName:
		var value bankNameCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		store.SetBankName(value.BankID, value.BankName)
	case checkpointKindMaxTx:
		var value maxTxCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		store.SetMaxTransaction(value.BankID, value.Account, value.AmountValue, value.AmountString)
	default:
		return fmt.Errorf("unknown maxbank checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *MaxBankCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyBankNames, clientID)
	delete(t.dirtyMaxTx, clientID)
}
