package filters

import (
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protowrappers"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

type AvgByTypeStats struct {
	Sum   float64
	Count int
}

type AvgByTypeFilter struct {
	inputExchange middleware.Middleware
	outputQueue   middleware.Middleware

	period1Stats        map[string]map[string]*AvgByTypeStats
	period2Transactions map[string]map[string][]*protobuf.AvgByTypeTransaction
}

type AvgByTypeFilterConfig struct {
	ID string

	InputExchangeName string
	OutputQueueName   string

	MomHost string
	MomPort int
}

func NewAvgByTypeFilter(config AvgByTypeFilterConfig) (*AvgByTypeFilter, error) {
	connSettings := middleware.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	inputKeys := []string{config.InputExchangeName + "." + config.ID}

	inputExchange, err := middleware.CreateExchangeMiddleware(config.InputExchangeName, inputKeys, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := middleware.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		inputExchange.Close()
		return nil, err
	}

	return &AvgByTypeFilter{
		inputExchange: inputExchange,
		outputQueue:   outputQueue,

		period1Stats:        make(map[string]map[string]*AvgByTypeStats),
		period2Transactions: make(map[string]map[string][]*protobuf.AvgByTypeTransaction),
	}, nil
}

func (f *AvgByTypeFilter) Run() error {
	go f.handleSignals()

	f.inputExchange.StartConsuming(
		func(msg middleware.Message, ack, nack func()) {

			moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
			if err != nil {
				nack()
				return
			}

			switch moneyLaundry.GetType() {

			case protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH:
				f.handleBatch(moneyLaundry, ack, nack)
			case protobuf.MessageType_EOF_:
				f.handleEOF(moneyLaundry, ack, nack)
			default:
				nack()
			}
		},
	)

	return nil
}

func (f *AvgByTypeFilter) handleFirstPeriod(clientID string, tx *protobuf.AvgByTypeTransaction) error {
	amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64)
	if err != nil {
		return err
	}

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
	return nil
}

func (f *AvgByTypeFilter) handleSecondPeriod(clientID string, tx *protobuf.AvgByTypeTransaction) error {
	paymentFormat := tx.GetPaymentFormat()
	if _, exists := f.period2Transactions[clientID]; !exists {
		f.period2Transactions[clientID] = make(map[string][]*protobuf.AvgByTypeTransaction)
	}

	f.period2Transactions[clientID][paymentFormat] = append(f.period2Transactions[clientID][paymentFormat], tx)
	return nil
}

func (f *AvgByTypeFilter) handleBatch(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	avgTypeBatch := moneyLaundry.GetAvgbytypeTransactionBatch()
	clientID := moneyLaundry.GetClientID()
	for _, tx := range avgTypeBatch.GetItems() {
		switch tx.GetPeriod() {
		case protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_FIRST:
			if err := f.handleFirstPeriod(clientID, tx); err != nil {
				nack()
				return
			}
		case protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_SECOND:
			if err := f.handleSecondPeriod(clientID, tx); err != nil {
				nack()
				return
			}
		default:
			nack()
			return
		}
	}

	ack()
}

func (f *AvgByTypeFilter) handleEOF(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {

	clientID := moneyLaundry.GetClientID()

	slog.Info(
		"processing avg by type EOF",
		"clientID", clientID,
	)

	statsByFormat, exists := f.period1Stats[clientID]

	if !exists {
		statsByFormat = make(map[string]*AvgByTypeStats)
	}

	transactionsByFormat := f.period2Transactions[clientID]
	resultBatcher := f.buildResultBatcher(clientID)

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

			if err := resultBatcher.Add(result); err != nil {
				nack()
				return
			}
		}
	}

	if err := resultBatcher.Flush(); err != nil {
		nack()
		return
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

func (f *AvgByTypeFilter) buildResultBatcher(clientID string) *batch.Batcher[*protobuf.AvgByTypeResult, *protobuf.AvgByTypeResultBatch] {
	sizer := protowrappers.ProtoSizer[*protobuf.AvgByTypeResult]()
	wrapper := protowrappers.WrapAvgByTypeResults
	resultBatch := batch.New(0, sizer, wrapper)
	onFlush := func(batch *protobuf.AvgByTypeResultBatch) error {
		return f.sendResultBatch(clientID, batch)
	}
	return batch.NewBatcher(resultBatch, onFlush)
}

func (f *AvgByTypeFilter) sendResultBatch(clientID string, batch *protobuf.AvgByTypeResultBatch) error {
	msg, err := serializer.SerializeProtoMessageWithClientID(clientID, batch, protobuf.MessageType_AVGBYTYPE_RESULT)
	if err != nil {
		return err
	}
	return f.outputQueue.Send(*msg)
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
	f.inputExchange.Close()
	f.outputQueue.Close()
}
