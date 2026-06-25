package avgbytype

import (
	"encoding/json"
	"fmt"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindPeriod1Stats = "period1Stats"
	checkpointKindPeriod2Txs   = "period2Txs"
)

type period1StatsCheckpointValue struct {
	Format string  `json:"format"`
	Sum    float64 `json:"sum"`
	Count  int     `json:"count"`
}

type period2TxsCheckpointValue struct {
	Format     string `json:"format"`
	Account    string `json:"account"`
	AmountPaid string `json:"amountPaid"`
}

type AvgByTypeCheckpointTracker struct {
	storeForClient              func(clientID string) *clientState
	dirtyPeriod1                map[string]map[string]bool
	dirtyPeriod2                map[string]map[string]bool
	lastCheckpointedPeriod2Idx  map[string]map[string]int
	pendingCheckpointPeriod2Idx map[string]map[string]int
}

func NewAvgByTypeCheckpointTracker(storeForClient func(clientID string) *clientState) *AvgByTypeCheckpointTracker {
	return &AvgByTypeCheckpointTracker{
		storeForClient:              storeForClient,
		dirtyPeriod1:                make(map[string]map[string]bool),
		dirtyPeriod2:                make(map[string]map[string]bool),
		lastCheckpointedPeriod2Idx:  make(map[string]map[string]int),
		pendingCheckpointPeriod2Idx: make(map[string]map[string]int),
	}
}

func (t *AvgByTypeCheckpointTracker) MarkPeriod1Changed(clientID, format string) {
	if t.dirtyPeriod1[clientID] == nil {
		t.dirtyPeriod1[clientID] = make(map[string]bool)
	}
	t.dirtyPeriod1[clientID][format] = true
}

func (t *AvgByTypeCheckpointTracker) MarkPeriod2Changed(clientID, format string) {
	if t.dirtyPeriod2[clientID] == nil {
		t.dirtyPeriod2[clientID] = make(map[string]bool)
	}
	t.dirtyPeriod2[clientID][format] = true
}

func (t *AvgByTypeCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	state := t.storeForClient(clientID)
	changes := make([]checkpoint.CheckpointChange, 0)

	if formats, ok := t.dirtyPeriod1[clientID]; ok {
		for format := range formats {
			if stats, exists := state.period1Stats[format]; exists {
				value, err := json.Marshal(period1StatsCheckpointValue{
					Format: format,
					Sum:    stats.Sum,
					Count:  stats.Count,
				})
				if err != nil {
					return nil, err
				}
				changes = append(changes, checkpoint.CheckpointChange{
					Kind:  checkpointKindPeriod1Stats,
					Key:   format,
					Value: json.RawMessage(value),
				})
			}
		}
	}

	if formats, ok := t.dirtyPeriod2[clientID]; ok {
		for format := range formats {
			if txs, exists := state.period2Transactions[format]; exists {
				from := t.lastCheckpointedIndex(clientID, format)
				if from > len(txs) {
					from = len(txs)
				}
				t.setPendingCheckpointIndex(clientID, format, from)
				for i := from; i < len(txs); i++ {
					tx := txs[i]
					value, err := json.Marshal(period2TxsCheckpointValue{
						Format:     format,
						Account:    tx.GetAccount(),
						AmountPaid: tx.GetAmountPaid(),
					})
					if err != nil {
						return nil, err
					}
					changes = append(changes, checkpoint.CheckpointChange{
						Kind:  checkpointKindPeriod2Txs,
						Key:   fmt.Sprintf("%s:%d", format, i),
						Value: json.RawMessage(value),
					})
				}
				t.setLastCheckpointedIndex(clientID, format, len(txs))
			}
		}
	}

	delete(t.dirtyPeriod1, clientID)
	delete(t.dirtyPeriod2, clientID)
	return changes, nil
}

func (t *AvgByTypeCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	period2Formats := make(map[string]struct{})

	for _, change := range changes {
		switch change.Kind {
		case checkpointKindPeriod1Stats:
			var value period1StatsCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err == nil {
				t.MarkPeriod1Changed(clientID, value.Format)
			}

		case checkpointKindPeriod2Txs:
			var value period2TxsCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err == nil {
				t.MarkPeriod2Changed(clientID, value.Format)
				period2Formats[value.Format] = struct{}{}
			}
		}
	}

	for format := range period2Formats {
		if previous, ok := t.pendingCheckpointIndex(clientID, format); ok {
			t.setLastCheckpointedIndex(clientID, format, previous)
			t.clearPendingCheckpointIndex(clientID, format)
		}
	}
	return nil
}

func (t *AvgByTypeCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	state := t.storeForClient(clientID)

	switch change.Kind {
	case checkpointKindPeriod1Stats:
		var value period1StatsCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		if state.period1Stats == nil {
			state.period1Stats = make(map[string]*AvgByTypeStats)
		}
		state.period1Stats[value.Format] = &AvgByTypeStats{
			Sum:   value.Sum,
			Count: value.Count,
		}

	case checkpointKindPeriod2Txs:
		var value period2TxsCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		if state.period2Transactions == nil {
			state.period2Transactions = make(map[string][]*protobuf.AvgByTypeTransaction)
		}
		tx := &protobuf.AvgByTypeTransaction{
			Account:    value.Account,
			AmountPaid: value.AmountPaid,
		}
		state.period2Transactions[value.Format] = append(state.period2Transactions[value.Format], tx)
		t.setLastCheckpointedIndex(clientID, value.Format, len(state.period2Transactions[value.Format]))

	default:
		return fmt.Errorf("unknown avgbytype checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *AvgByTypeCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyPeriod1, clientID)
	delete(t.dirtyPeriod2, clientID)
	delete(t.lastCheckpointedPeriod2Idx, clientID)
	delete(t.pendingCheckpointPeriod2Idx, clientID)
}

func (t *AvgByTypeCheckpointTracker) lastCheckpointedIndex(clientID, format string) int {
	if t.lastCheckpointedPeriod2Idx[clientID] == nil {
		return 0
	}
	return t.lastCheckpointedPeriod2Idx[clientID][format]
}

func (t *AvgByTypeCheckpointTracker) setLastCheckpointedIndex(clientID, format string, index int) {
	if t.lastCheckpointedPeriod2Idx[clientID] == nil {
		t.lastCheckpointedPeriod2Idx[clientID] = make(map[string]int)
	}
	t.lastCheckpointedPeriod2Idx[clientID][format] = index
}

func (t *AvgByTypeCheckpointTracker) setPendingCheckpointIndex(clientID, format string, index int) {
	if t.pendingCheckpointPeriod2Idx[clientID] == nil {
		t.pendingCheckpointPeriod2Idx[clientID] = make(map[string]int)
	}
	t.pendingCheckpointPeriod2Idx[clientID][format] = index
}

func (t *AvgByTypeCheckpointTracker) pendingCheckpointIndex(clientID, format string) (int, bool) {
	if t.pendingCheckpointPeriod2Idx[clientID] == nil {
		return 0, false
	}
	index, ok := t.pendingCheckpointPeriod2Idx[clientID][format]
	return index, ok
}

func (t *AvgByTypeCheckpointTracker) clearPendingCheckpointIndex(clientID, format string) {
	if t.pendingCheckpointPeriod2Idx[clientID] == nil {
		return
	}
	delete(t.pendingCheckpointPeriod2Idx[clientID], format)
	if len(t.pendingCheckpointPeriod2Idx[clientID]) == 0 {
		delete(t.pendingCheckpointPeriod2Idx, clientID)
	}
}
