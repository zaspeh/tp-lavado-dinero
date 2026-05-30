package sender

type Sender[V any] interface {
	Add(clientID string) error
	Flush(clientID string) error
	Cleanup(clientID string) error
	Close() error
}
