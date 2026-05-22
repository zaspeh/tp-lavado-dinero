package filters

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeStats struct {
	Sum   float64
	Count int
}

type AvgByTypeFilter struct {
	inputQueue  middleware.Middleware
	outputQueue middleware.Middleware

	period1Stats map[string]map[string]*AvgByTypeStats

	period2Transactions map[string]map[string][]*protobuf.AvgByTypeTransaction
}

type AvgByTypeFilterConfig struct {
	InputQueueName  string
	OutputQueueName string

	MomHost string
	MomPort int
}

func NewAvgByTypeFilter(config AvgByTypeFilterConfig) (*AvgByTypeFilter, error) {

	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputQueue, err := middleware.CreateQueueMiddleware(
		config.InputQueueName,
		connSettings,
	)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(
		config.OutputQueueName,
		connSettings,
	)
	if err != nil {
		inputQueue.Close()
		return nil, err
	}

	return &AvgByTypeFilter{
		inputQueue:          inputQueue,
		outputQueue:         outputQueue,
		period1Stats:        make(map[string]map[string]*AvgByTypeStats),
		period2Transactions: make(map[string]map[string][]*protobuf.AvgByTypeTransaction),
	}, nil
}

func (f *AvgByTypeFilter) Run() error {
	go f.handleSignals()

	f.inputQueue.StartConsuming(
		func(msg middleware.Message, ack, nack func()) {

			moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
			if err != nil {
				nack()
				return
			}

			switch moneyLaundry.GetType() {

			case protobuf.MessageType_AVGBYTYPE_FIRST_PERIOD:
				f.handleFirstPeriod(moneyLaundry, ack, nack)

			case protobuf.MessageType_AVGBYTYPE_SECOND_PERIOD:
				f.handleSecondPeriod(moneyLaundry, ack, nack)

			case protobuf.MessageType_EOF_:
				f.handleEOF(moneyLaundry, ack, nack)

			default:
				nack()
			}
		},
	)

	return nil
}

func (f *AvgByTypeFilter) handleFirstPeriod(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	tx, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.AvgByTypeTransaction{})
	if err != nil {
		nack()
		return
	}

	amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64)
	if err != nil {
		nack()
		return
	}

	clientID := moneyLaundry.GetClientID()
	paymentFormat := tx.GetPaymentFormat()

	if _, exists := f.period1Stats[clientID]; !exists {
		f.period1Stats[clientID] = make(map[string]*AvgByTypeStats)
	}

	stats, exists := f.period1Stats[clientID][paymentFormat]

	if !exists {
		stats = &AvgByTypeStats{}
		f.period1Stats[clientID][paymentFormat] = stats
	}

	stats.Sum += amount
	stats.Count++

	ack()
}

func (f *AvgByTypeFilter) handleSecondPeriod(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	tx, err := serializer.DeserializeTransaction(moneyLaundry.GetPayload(), &protobuf.AvgByTypeTransaction{})
	if err != nil {
		nack()
		return
	}

	clientID := moneyLaundry.GetClientID()
	paymentFormat := tx.GetPaymentFormat()

	if _, exists := f.period2Transactions[clientID]; !exists {
		f.period2Transactions[clientID] = make(map[string][]*protobuf.AvgByTypeTransaction)
	}

	f.period2Transactions[clientID][paymentFormat] = append(f.period2Transactions[clientID][paymentFormat], tx)

	ack()
}

func (f *AvgByTypeFilter) handleEOF(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	clientID := moneyLaundry.GetClientID()

	statsByFormat, exists := f.period1Stats[clientID]

	if !exists {
		statsByFormat = make(map[string]*AvgByTypeStats)
	}

	transactionsByFormat := f.period2Transactions[clientID]

	for paymentFormat, stats := range statsByFormat {

		if stats.Count == 0 {
			continue
		}

		average := stats.Sum / float64(stats.Count)

		threshold := average / 100

		for _, tx := range transactionsByFormat[paymentFormat] {

			amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64)
			if err != nil {
				continue
			}

			if amount >= threshold {
				continue
			}

			result := &protobuf.AvgByTypeResult{
				Account:    tx.GetAccount(),
				AmountPaid: tx.GetAmountPaid(),
			}

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

func (f *AvgByTypeFilter) handleSignals() {

	signals := make(chan os.Signal, 1)

	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-signals

	slog.Info("shutdown signal received")

	f.inputQueue.Close()
	f.outputQueue.Close()
}
