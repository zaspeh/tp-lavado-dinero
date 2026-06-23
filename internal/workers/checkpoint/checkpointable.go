package checkpoint

import "encoding/json"

type CheckpointLogEntry struct {
	Seq     uint64             `json:"seq"`
	Batches []string           `json:"batches"`
	Changes []CheckpointChange `json:"changes"`
}

type CheckpointChange struct {
	Kind  string          `json:"kind"`
	Key   string          `json:"key"`
	Value json.RawMessage `json:"value"`
}

type Checkpointable interface {
	ClearClientState(clientID string) error
}

type ChangeCheckpointable interface {
	Checkpointable
	DrainChanges(clientID string) ([]CheckpointChange, error)
	ApplyChange(clientID string, change CheckpointChange) error
}

type RestorableChangeCheckpointable interface {
	RestoreChanges(clientID string, changes []CheckpointChange) error
}

type EntityCheckpointable interface {
	Checkpointable
	SerializeEntity(clientID, entityID string) ([]byte, error)
	LoadEntity(clientID, entityID string, data []byte) error
	ListEntities(clientID string) ([]string, error)
}
