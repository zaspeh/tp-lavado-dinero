package processor

type Processor[T, V any] interface {
	Process(clientID string, item T) ([]V, error)
}

type StatefulProcessor[T, V any] interface {
	Processor[T, V]
	Finalize(clientID string, yield func(result V) error) error
	Cleanup(clientID string) error
}
