package conversionjoin

import (
	"encoding/json"
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

const (
	checkpointKindCount = "count"
)

type countCheckpointValue struct {
	Count int `json:"count"`
}

type ConversionJoinCheckpointTracker struct {
	clientResults *map[string]int
	dirtyCounts  map[string]bool
}

func NewConversionJoinCheckpointTracker(clientResults *map[string]int) *ConversionJoinCheckpointTracker {
	return &ConversionJoinCheckpointTracker{
		clientResults: clientResults,
		dirtyCounts:   make(map[string]bool),
	}
}

func (t *ConversionJoinCheckpointTracker) MarkCountChanged(clientID string) {
	t.dirtyCounts[clientID] = true
}

func (t *ConversionJoinCheckpointTracker) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	if !t.dirtyCounts[clientID] {
		return nil, nil
	}

	count := (*t.clientResults)[clientID]

	value, err := json.Marshal(countCheckpointValue{Count: count})
	if err != nil {
		return nil, err
	}

	changes := []checkpoint.CheckpointChange{
		{
			Kind:  checkpointKindCount,
			Key:   clientID,
			Value: json.RawMessage(value),
		},
	}

	delete(t.dirtyCounts, clientID)
	return changes, nil
}

func (t *ConversionJoinCheckpointTracker) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	for _, change := range changes {
		switch change.Kind {
		case checkpointKindCount:
			var value countCheckpointValue
			if err := json.Unmarshal(change.Value, &value); err != nil {
				return err
			}
			(*t.clientResults)[clientID] = value.Count
			t.MarkCountChanged(clientID)
		}
	}
	return nil
}

func (t *ConversionJoinCheckpointTracker) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	switch change.Kind {
	case checkpointKindCount:
		var value countCheckpointValue
		if err := json.Unmarshal(change.Value, &value); err != nil {
			return err
		}
		(*t.clientResults)[clientID] = value.Count
	default:
		return fmt.Errorf("unknown conversionjoin checkpoint change kind: %s", change.Kind)
	}
	return nil
}

func (t *ConversionJoinCheckpointTracker) ClearClient(clientID string) {
	delete(t.dirtyCounts, clientID)
}