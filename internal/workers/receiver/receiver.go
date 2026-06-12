package receiver

type MessageType int

const (
	DataMessage MessageType = iota
	EOFMessage
	CleanupMessage
)

// Event Necesario para manejar los tipos de mensajes entrantes
type Event[T any] struct {
	EventID  string
	Type     MessageType
	ClientID string
	Data     []T
	EOFCount uint64
}

type Receiver[T any] interface {
	Receive(handler func(event Event[T]) error) error
	Close() error
}
