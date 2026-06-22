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

	// AckFn y Nack son las funciones reales del broker que el handler
	// es responsable de invocar (típicamente vía el checkpoint manager
	// después de persistir). Si ninguna se invoca, RabbitMQ redelivery
	// el mensaje cuando el consumer muere.
	AckFn func()
	Nack  func()
}

type Receiver[T any] interface {
	Receive(handler func(event Event[T]) error) error
	Close() error
}
