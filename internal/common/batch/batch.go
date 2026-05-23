package batch

const (
	defaultBatchSize = 8192 // 8KB
)

// Sizer es cualquier tipo del que se pueda obtener su peso en bytes.
type Sizer[T any] func(T) int

// Wrapper construye el envoltorio V a partir de los items acumulados.
type Wrapper[T any, V any] func(items []T) V

type Batch[T any, V any] struct {
	maxWeight     int
	currentWeight int
	items         []T
	sizer         Sizer[T]
	wrapper       Wrapper[T, V]
}

func New[T any, V any](maxWeight int, sizer Sizer[T], wrapper Wrapper[T, V]) *Batch[T, V] {
	if maxWeight <= 0 {
		maxWeight = defaultBatchSize
	}

	return &Batch[T, V]{
		maxWeight: maxWeight,
		items:     make([]T, 0),
		sizer:     sizer,
		wrapper:   wrapper,
	}
}

func (b *Batch[T, V]) TryAdd(item T) bool {
	w := b.sizer(item)
	if b.currentWeight+w > b.maxWeight && len(b.items) > 0 {
		return false
	}
	b.items = append(b.items, item)
	b.currentWeight += w
	return true
}

func (b *Batch[T, V]) Flush() V {
	result := b.wrapper(b.items)
	b.clear()
	return result
}

func (b *Batch[T, V]) IsEmpty() bool {
	return b.currentWeight == 0
}

func (b *Batch[T, V]) clear() {
	b.items = b.items[:0]
	b.currentWeight = 0
}
