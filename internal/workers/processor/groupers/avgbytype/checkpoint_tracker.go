package avgbytype

import (
	"encoding/json"
	"fmt"
	"log/slog"

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
	Format      string `json:"format"`
	Account     string `json:"account"`
	AmountPaid  string `json:"amountPaid"`
}

type AvgByTypeCheckpointTracker struct {
	storeForClient func(clientID string) *clientState
	dirtyPeriod1   map[string]map[string]bool
	dirtyPeriod2   map[string]map[string]bool
}

func NewAvgByTypeCheckpointTracker(storeForClient func(clientID string) *clientState) *AvgByTypeCheckpointTracker {
	return &AvgByTypeCheckpointTracker{
		storeForClient: storeForClient,
		dirtyPeriod1:   make(map[string]map[string]bool),
		dirtyPeriod2:   make(map[string]map[string]bool),
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
				for _, tx := range txs {
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
						Key:   fmt.Sprintf("%s:%s", format, tx.GetAccount()),
						Value: json.RawMessage(value),
					})
				}
			}
		}
	}

	delete(t.dirtyPeriod1, clientID)
	delete(t.dirtyPeriod2, clientID)
	return changes, nil
}

func (t *AvgByTypeCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	state := t.storeForClient(clientID)

	for _, change := range changes {
		switch change.Kind {
		case checkpointKindPeriod1Stats:
			var value period1StatsCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				slog.Error("AvgByTypeCheckpointTracker RestoreChanges: failed to unmarshal period1Stats", "clientID", clientID, "error", err)
				return err
			}
			if state.period1Stats == nil {
				state.period1Stats = make(map[string]*AvgByTypeStats)
			}
			state.period1Stats[value.Format] = &AvgByTypeStats{
				Sum:   value.Sum,
				Count: value.Count,
			}
			t.MarkPeriod1Changed(clientID, value.Format)

		case checkpointKindPeriod2Txs:
			var value period2TxsCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				slog.Error("AvgByTypeCheckpointTracker RestoreChanges: failed to unmarshal period2Txs", "clientID", clientID, "error", err)
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
			t.MarkPeriod2Changed(clientID, value.Format)
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
			slog.Error("AvgByTypeCheckpointTracker ApplyChange: failed to unmarshal period1Stats", "clientID", clientID, "error", err)
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
			slog.Error("AvgByTypeCheckpointTracker ApplyChange: failed to unmarshal period2Txs", "clientID", clientID, "error", err)
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

	default:
		err := fmt.Errorf("unknown avgbytype checkpoint change kind: %s", change.Kind)
		slog.Error("AvgByTypeCheckpointTracker ApplyChange: unknown change kind", "clientID", clientID, "kind", change.Kind, "error", err)
		return err
	}
	return nil
}

func (t *AvgByTypeCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyPeriod1, clientID)
	delete(t.dirtyPeriod2, clientID)
}