package sender

type Sender[V any] interface {
	Add(clientID string, item V) error
	Flush(clientID string) error
	Cleanup(clientID string) error
	Close() error
}
