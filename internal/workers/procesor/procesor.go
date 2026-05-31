package procesor

type Procesor[T, V any] interface {
	Process(clientID string, item T) (V, error)
}
