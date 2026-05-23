package batch

func ForEachFlushed[T any, V any](b *Batch[T, V], next func() (T, bool), onFlush func(V) error) error {
	for {
		item, ok := next()
		if !ok {
			break
		}
		if !b.TryAdd(item) {
			if err := onFlush(b.Flush()); err != nil {
				return err
			}
			b.TryAdd(item)
		}
	}

	// Vaciamos el remanente
	if !b.IsEmpty() {
		return onFlush(b.Flush())
	}
	return nil
}
