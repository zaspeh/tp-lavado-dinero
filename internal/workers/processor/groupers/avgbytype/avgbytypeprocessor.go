package avgbytype

import (
	"log/slog"
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type AvgByTypeStats struct {
	Sum   float64
	Count int
}

type clientState struct {
	period1Stats        map[string]*AvgByTypeStats
	period2Transactions map[string][]*protobuf.AvgByTypeTransaction
}

type AvgByTypeProcessor struct {
	clients  map[string]*clientState
	tracker  *AvgByTypeCheckpointTracker
}

func NewAvgByTypeProcessor() *AvgByTypeProcessor {
	processor := &AvgByTypeProcessor{
		clients: make(map[string]*clientState),
	}
	processor.tracker = NewAvgByTypeCheckpointTracker(processor.getOrCreateClientState)
	return processor
}

func (p *AvgByTypeProcessor) Process(clientID string, item *protobuf.AvgByTypeTransaction, cm *checkpoint.CheckpointManager) error {
	state := p.getOrCreateClientState(clientID)

	slog.Debug("AvgByTypeProcessor Process", "clientID", clientID, "period", item.GetPeriod(), "account", item.GetAccount(), "amount", item.GetAmountPaid(), "paymentFormat", item.GetPaymentFormat())

	switch item.GetPeriod() {
	case protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_FIRST:
		return p.processFirstPeriod(state, item, clientID, cm)
	case protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_SECOND:
		return p.processSecondPeriod(state, item, clientID, cm)
	default:
		return nil
	}
}

func (p *AvgByTypeProcessor) processFirstPeriod(state *clientState, tx *protobuf.AvgByTypeTransaction, clientID string, cm *checkpoint.CheckpointManager) error {
	amount, err := strconv.ParseFloat(tx.GetAmountPaid(), 64)
	if err != nil {
		return err
	}

	paymentFormat := tx.GetPaymentFormat()
	if state.period1Stats == nil {
		state.period1Stats = make(map[string]*AvgByTypeStats)
	}

	stats, exists := state.period1Stats[paymentFormat]
	if !exists {
		stats = &AvgByTypeStats{}
		state.period1Stats[paymentFormat] = stats
	}

	stats.Sum += amount
	stats.Count++

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "period1")
	}
	p.tracker.MarkPeriod1Changed(clientID, paymentFormat)
	return nil
}

func (p *AvgByTypeProcessor) processSecondPeriod(state *clientState, tx *protobuf.AvgByTypeTransaction, clientID string, cm *checkpoint.CheckpointManager) error {
	paymentFormat := tx.GetPaymentFormat()
	if state.period2Transactions == nil {
		state.period2Transactions = make(map[string][]*protobuf.AvgByTypeTransaction)
	}

	state.period2Transactions[paymentFormat] = append(state.period2Transactions[paymentFormat], tx)

	if cm != nil {
		cm.NotifyEntityChanged(clientID, "period2")
	}
	p.tracker.MarkPeriod2Changed(clientID, paymentFormat)
	return nil
}

func (p *AvgByTypeProcessor) Finalize(clientID string, yield func(result *protobuf.AvgByTypeResult) error) (uint64, error) {
	state, exists := p.clients[clientID]
	if !exists {
		slog.Debug("AvgByTypeProcessor Finalize: no state for client", "clientID", clientID)
		return 0, nil
	}

	statsByFormat := state.period1Stats
	if statsByFormat == nil {
		statsByFormat = make(map[string]*AvgByTypeStats)
	}

	transactionsByFormat := state.period2Transactions
	resultCount := uint64(0)

	slog.Debug("AvgByTypeProcessor Finalize", "clientID", clientID, "period1Formats", len(statsByFormat), "period2Formats", len(transactionsByFormat))

	for paymentFormat, stats := range statsByFormat {
		if stats.Count == 0 {
			continue
		}

		average := stats.Sum / float64(stats.Count)
		threshold := average / 100

		slog.Debug("AvgByTypeProcessor Finalize: processing format", "clientID", clientID, "paymentFormat", paymentFormat, "average", average, "threshold", threshold, "period1Count", stats.Count)

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

			if err := yield(result); err != nil {
				return 0, err
			}
			resultCount++
		}
	}

	slog.Debug("AvgByTypeProcessor Finalize: done", "clientID", clientID, "resultCount", resultCount)
	return resultCount, nil
}

func (p *AvgByTypeProcessor) Cleanup(clientID string) error {
	slog.Debug("AvgByTypeProcessor Cleanup", "clientID", clientID)
	p.tracker.ClearClient(clientID)
	delete(p.clients, clientID)
	return nil
}

func (p *AvgByTypeProcessor) getOrCreateClientState(clientID string) *clientState {
	state, exists := p.clients[clientID]
	if !exists {
		state = &clientState{}
		p.clients[clientID] = state
	}
	return state
}

func (p *AvgByTypeProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}

func (p *AvgByTypeProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return p.tracker.DrainChanges(clientID)
}

func (p *AvgByTypeProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return p.tracker.RestoreChanges(clientID, changes)
}

func (p *AvgByTypeProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return p.tracker.ApplyChange(clientID, change)
}
