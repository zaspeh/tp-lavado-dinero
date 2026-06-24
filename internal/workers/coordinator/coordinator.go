package coordinator

// Funcion del estilo callback usada para que una vez que los
// nodos hermanos hayan recibido la totalidad de los mensajes
// se haga flush correspondiente
type FlushHandler func(clientID string, survivorCount uint64, eventID string) error

type Coordinator interface {
	Run() error
	Close() error
	RecordBatch(clientID, batchID string, processed, survivors uint64) error
	HasSeenBatch(clientID, batchID string) bool
	IsLeader() bool
	SetFlushHandler(handler FlushHandler)
	HandleLocalEOF(clientID string, eofCount uint64, eventID string) error
	ReachedEOFAmount(clientID string) bool
	ClearClient(clientID string) error
	BroadcastCleanup(clientID string) error
}
