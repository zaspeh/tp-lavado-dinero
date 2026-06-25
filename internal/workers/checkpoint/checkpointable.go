package checkpoint

import "encoding/json"

type CheckpointLogEntry struct {
	Seq             uint64             `json:"seq"`
	Batches         []string           `json:"batches"`
	Changes         []CheckpointChange `json:"changes"`
	ProcessedCounts []uint64           `json:"processedCounts"`
}

type CheckpointChange struct {
	Kind  string          `json:"kind"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type Checkpointable interface {
	ClearClientState(clientID string) error
	DrainChanges(clientID string) ([]CheckpointChange, error)
	RestoreChanges(clientID string, changes []CheckpointChange) error
	ApplyChange(clientID string, change CheckpointChange) error
}
