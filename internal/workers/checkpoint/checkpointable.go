package checkpoint

type Checkpointable interface {
	SerializeEntity(clientID, entityID string) ([]byte, error)
	LoadEntity(clientID, entityID string, data []byte) error
	ListEntities(clientID string) ([]string, error)
	ClearClientState(clientID string) error
}
