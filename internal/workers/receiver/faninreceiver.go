package receiver

import (
	"log/slog"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type WrapEvent[T any] struct {
	event Event[T]
	ack   func()
	nack  func()
}

type FanInReceiver[T any] struct {
	inputs            []m.Middleware
	inputsIDs         []string
	targetMessageType protobuf.MessageType
	extractData       func(*protobuf.MoneyLaundry, string) []T
	internalChan      chan WrapEvent[T]
}

func NewFanInReceiver[T any](
	inputs []m.Middleware,
	inputsIDs []string,
	targetMessageType protobuf.MessageType,
	extractData func(*protobuf.MoneyLaundry, string) []T,
) Receiver[T] {
	return &FanInReceiver[T]{
		inputs:            inputs,
		inputsIDs:         inputsIDs,
		targetMessageType: targetMessageType,
		extractData:       extractData,
	}
}

func (r *FanInReceiver[T]) Receive(handler func(event Event[T]) error) error {
	var internalChan = make(chan WrapEvent[T])
	for i, input := range r.inputs {
		inputID := r.inputsIDs[i]
		go input.StartConsuming(func(msg m.Message, ack, nack func()) {
			r.consume(msg, ack, nack, inputID, internalChan)
		})
	}

	for wrapEvent := range internalChan {
		if err := handler(wrapEvent.event); err != nil {
			slog.Error("Handler error", "error", err)
			wrapEvent.nack()
			return err
		}
	}
	return nil
}

func (r *FanInReceiver[T]) consume(msg m.Message, ack, nack func(), inputID string, internalChan chan<- WrapEvent[T]) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		slog.Error("Failed to deserialize wrapper", "error", err)
		nack()
		return
	}
	event := Event[T]{
		EventID:  moneyLaundry.GetBatchID(),
		ClientID: moneyLaundry.GetClientID(),
		AckFn:    ack,
		Nack:     nack,
	}
	switch moneyLaundry.GetType() {
	case protobuf.MessageType_EOF_:
		event.Type = EOFMessage
		event.EOFCount = moneyLaundry.GetEofMessage().GetTotalTransactions()
	case protobuf.MessageType_CLEANUP:
		event.Type = CleanupMessage
	case r.targetMessageType:
		event.Type = DataMessage
		event.Data = r.extractData(moneyLaundry, inputID)
	default:
		slog.Debug("Ignored unknown message type", "type", moneyLaundry.GetType())
		ack()
		return
	}
	internalChan <- WrapEvent[T]{event: event, ack: ack, nack: nack}
}

func (r *FanInReceiver[T]) Close() error {
	for _, input := range r.inputs {

		// TODO: Aseguramos cierre de todos auqnue haya errores hacer loggeo
		_ = input.Close()
	}
	return nil
}
