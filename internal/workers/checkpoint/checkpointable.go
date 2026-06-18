package checkpoint

type Checkpointable interface {
	GetWorkerName() string
	GetWorkerID() int
	GetClientState(clientID string) ([]byte, error)
	LoadClientState(clientID string, data []byte) error
	ClearClientState(clientID string) error
}
