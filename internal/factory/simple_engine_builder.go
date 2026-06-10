package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/coordinator"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/engine"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/processor"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/receiver"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/sender"
)

type singleReceiverSingleSenderEngineConfig[T, V, R any] struct {
	InputQueue          middleware.Middleware
	OutputQueue         middleware.Middleware
	ReceivedMessageType protobuf.MessageType
	Extractor           func(*protobuf.MoneyLaundry) []T
	Wrapper             batch.Wrapper[V, R]
	Sizer               batch.Sizer[V]
	Inserter            func(clientID string, batch R) (middleware.Message, error)
	Processor           processor.Processor[T, V]
	Coordinator         coordinator.Coordinator
}

func buildSingleReceiverSingleSenderEngine[T, V, R any](
	cfg singleReceiverSingleSenderEngineConfig[T, V, R],
) engine.Engine {
	singleReceiver := receiver.NewSingleReceiver(
		cfg.InputQueue,
		cfg.ReceivedMessageType,
		cfg.Extractor,
	)

	singleSender := sender.NewSingleSender(
		cfg.OutputQueue,
		cfg.Wrapper,
		cfg.Sizer,
		0,
		cfg.Inserter,
	)

	return engine.NewStatelessEngine(
		singleReceiver,
		singleSender,
		cfg.Processor,
		cfg.Coordinator,
	)
}
