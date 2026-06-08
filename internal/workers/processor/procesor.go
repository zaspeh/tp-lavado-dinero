package processor

type Processor[T, V any] interface {
	Process(clientID string, item T) ([]V, error)
}

type StatefulProcessor[T, V any] interface {
	Process(clientID string, item T) error
	Finalize(clientID string, yield func(result V) error) (uint64, error)
	Cleanup(clientID string) error
}
