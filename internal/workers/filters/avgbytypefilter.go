package filters

import (
	"strconv"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeStats struct {
	Sum   float64
	Count int
}

type AvgByTypeFilter struct {
	inputExchange middleware.Middleware
	outputQueue   middleware.Middleware

	period1Start time.Time
	period1End   time.Time

	period2Start time.Time
	period2End   time.Time

	period1Stats map[string]*AvgByTypeStats

	period2Transactions map[string][]*protobuf.AvgByTypeTransaction
}

type AvgByTypeFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int

	Period1Start time.Time
	Period1End   time.Time

	Period2Start time.Time
	Period2End   time.Time
}

func (af *AvgByTypeFilter) Run() error {
	af.inputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
		if err != nil {
			nack()
			return
		}

		switch moneyLaundry.GetType() {

		case protobuf.MessageType_AVGBYTYPETRANSACTION:
			af.handleTransaction(moneyLaundry, ack, nack)

		case protobuf.MessageType_EOF_:
			af.handleEOF(moneyLaundry, ack, nack)

		default:
			nack()
		}
	},
	)

	return nil
}

func (f *AvgByTypeFilter) handleMessage(msg middleware.Message, ack, nack func()) {
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {

	case protobuf.MessageType_AVGBYTYPETRANSACTION:
		f.handleTransaction(moneyLaundry, ack, nack)

	case protobuf.MessageType_EOF_:
		f.handleEOF(moneyLaundry, ack, nack)

	default:
		nack()
	}
}

func (f *AvgByTypeFilter) handleTransaction(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	tx, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.AvgByTypeTransaction{})

	if err != nil {
		nack()
		return
	}

	clientID := moneyLaundry.GetClientID()

	timestamp := tx.GetTimestamp().AsTime()

	if timestamp.After(f.period1Start) && timestamp.Before(f.period1End) {

		amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64) // está bien parsear a float? son solo en USD
		if err != nil {
			nack()
			return
		}

		stats, exists := f.period1Stats[clientID]
		if !exists {
			stats = &AvgByTypeStats{}
			f.period1Stats[clientID] = stats
		}

		stats.Sum += amount
		stats.Count++

		ack()
		return
	}

	if timestamp.After(f.period2Start) && timestamp.Before(f.period2End) {
		f.period2Transactions[clientID] = append(f.period2Transactions[clientID], tx)
	}

	ack()
}

func (f *AvgByTypeFilter) handleEOF(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	clientID := moneyLaundry.GetClientID()

	stats, exists := f.period1Stats[clientID]

	if !exists || stats.Count == 0 {

		if err := f.outputQueue.Send(middleware.Message{Body: moneyLaundry.ProtoReflect().Bytes()}); err != nil {
			nack()
			return
		}

		ack()
		return
	}

	average := stats.Sum / float64(stats.Count)

	threshold := average / 100

	for _, tx := range f.period2Transactions[clientID] {

		amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64)
		if err != nil {
			continue
		}

		if amount >= threshold {
			continue
		}

		result := &protobuf.AvgByTypeResult{Account: tx.GetAccount(), AmountPaid: tx.GetAmountPaid()}

		msg, err := serializer.SerializeProtoMessageWithClientID(clientID, result, protobuf.MessageType_AVGBYTYPE_RESULT)
		if err != nil {
			nack()
			return
		}

		if err := f.outputQueue.Send(*msg); err != nil {
			nack()
			return
		}
	}

	delete(f.period1Stats, clientID)
	delete(f.period2Transactions, clientID)

	eofMsg, err := serializer.SerializeProtoMessageWithClientID(clientID, &protobuf.EOF{}, protobuf.MessageType_EOF_)
	if err != nil {
		nack()
		return
	}

	if err := f.outputQueue.Send(*eofMsg); err != nil {
		nack()
		return
	}

	ack()
}
