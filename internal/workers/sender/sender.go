package sender

type Sender interface {
	Add(clientID string) error
	Flush(clientID string) error
	Cleanup(clientID string) error
	Close() error
}
