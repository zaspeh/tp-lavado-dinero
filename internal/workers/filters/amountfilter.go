package filters

import (
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AmountFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	AmountToFilter float64
}

type AmountFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	AmountToFilter float64
}

func NewAmountFilter(config AmountFilterConfig) (*AmountFilter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(config.InputQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &AmountFilter{
		inputQueue:     inputQueue,
		outputQueue:    outputQueue,
		AmountToFilter: config.AmountToFilter,
	}, nil
}

func (af *AmountFilter) Run() error {
	af.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		moneyLaundering, err := serializer.DeserializeMoneyLaundering(msg)
		if err != nil {
			nack()
			return
		}

		switch moneyLaundering.Type {

		case protobuf.MessageType_MICROTRANSACTION:
			af.handleMicrotransactionMessage(moneyLaundering, msg, ack, nack)

		case protobuf.MessageType_EOF:
			if err := af.outputQueue.Send(msg); err != nil {
				nack()
				return
			}

			ack()

		default:
			nack()
		}
	},
	)

	return nil
}

func (af *AmountFilter) handleMicrotransactionMessage(moneyLaundering *protobuf.MoneyLaundry, msg middleware.Message, ack, nack func()) {
	microtransaction, err := serializer.DeserializeTransaction(moneyLaundering.Payload, &protobuf.Microtransaction{})
	if err != nil {
		nack()
		return
	}

	amount, err := strconv.ParseFloat(microtransaction.GetAmountPaid(), 64)
	if err != nil {
		nack()
		return
	}

	if amount < af.AmountToFilter {
		if err := af.outputQueue.Send(msg); err != nil {
			nack()
			return
		}
	}

	ack()
}
