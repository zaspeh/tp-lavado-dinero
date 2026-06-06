package receiver

import m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type FanInReceiver[T any] struct {
	inputs []m.Middleware
}

func NewFanInReceiver[T any](inputs []m.Middleware) Receiver[T] {
	return &FanInReceiver[T]{inputs: inputs}
}

func (r *FanInReceiver[T]) Receive(handler func(event Event[T]) error) error {
	return nil
}

func (r *FanInReceiver[T]) Close() error {
	for _, input := range r.inputs {

		// TODO: Aseguramos cierre de todos auqnue haya errores hacer loggeo
		_ = input.Close()
	}
	return nil
}
