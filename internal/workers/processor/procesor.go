package processor

type Processor[T, V any] interface {
	Process(clientID string, item T) ([]V, error)
}
