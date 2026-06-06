package receiver

import m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type DualReceiver[T any] struct {
	inputs []m.Middleware
}

func NewDualReceiver[T any](inputs []m.Middleware) Receiver[T] {
	return &DualReceiver[T]{inputs: inputs}
}

func (r *DualReceiver[T]) Receive(handler func(event Event[T]) error) error {
	return nil
}

func (r *DualReceiver[T]) Close() error {
	return nil
}
