package receiver

import (
	"log/slog"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

type SingleReceiver[T any] struct {
	input             m.Middleware
	targetMessageType protobuf.MessageType
}

func NewSingleReceiver[T any](input m.Middleware) Receiver[T] {
	return &SingleReceiver[T]{
		input: input,
	}
}

func (r *SingleReceiver[T]) Receive(handler func(event Event[T]) error) error {
	return r.input.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
		if err != nil {
			slog.Error("Failed to deserialize wrapper", "error", err)
			nack()
			return
		}

		event := Event[T]{ClientID: moneyLaundry.GetClientID()}

		switch moneyLaundry.GetType() {
		case protobuf.MessageType_EOF_:
			event.Type = EOFMessage
			event.EOFCount = moneyLaundry.GetEofMessage().GetTotalTransactions()
		case protobuf.MessageType_CLEANUP:
			event.Type = CleanupMessage
		case r.targetMessageType:
			event.Type = DataMessage
			// event.Data = unmarshaler(moneyLaundry)
		default:
			slog.Debug("Ignored unknown message type", "type", moneyLaundry.GetType())
			ack()
			return
		}

		if err := handler(event); err != nil {
			slog.Error("Handler error", "error", err)
			nack()
			return
		}

		ack()
	})
}

func (r *SingleReceiver[T]) Close() error {
	return r.input.Close()
}
