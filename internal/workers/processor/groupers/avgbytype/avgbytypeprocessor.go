package avgbytype

import (
	"encoding/json"
	"fmt"
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
	clients map[string]*clientState
}

type period1Entity struct {
	Format string  `json:"format"`
	Sum    float64 `json:"sum"`
	Count  int     `json:"count"`
}

type period2Entity struct {
	Format     string `json:"format"`
	Account    string `json:"account"`
	AmountPaid string `json:"amountPaid"`
}

func NewAvgByTypeProcessor() *AvgByTypeProcessor {
	return &AvgByTypeProcessor{
		clients: make(map[string]*clientState),
	}
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

func (p *AvgByTypeProcessor) ListEntities(clientID string) ([]string, error) {
	state, ok := p.clients[clientID]
	if !ok {
		return nil, nil
	}
	entities := make([]string, 0, 2)
	if state.period1Stats != nil && len(state.period1Stats) > 0 {
		entities = append(entities, "period1")
	}
	if state.period2Transactions != nil && len(state.period2Transactions) > 0 {
		entities = append(entities, "period2")
	}
	return entities, nil
}

func (p *AvgByTypeProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	state, ok := p.clients[clientID]
	if !ok {
		return nil, fmt.Errorf("client state not found")
	}

	switch entityID {
	case "period1":
		entities := make([]period1Entity, 0, len(state.period1Stats))
		for format, stats := range state.period1Stats {
			entities = append(entities, period1Entity{
				Format: format,
				Sum:    stats.Sum,
				Count:  stats.Count,
			})
		}
		return json.Marshal(entities)
	case "period2":
		var entities []period2Entity
		for format, txs := range state.period2Transactions {
			for _, tx := range txs {
				entities = append(entities, period2Entity{
					Format:     format,
					Account:    tx.GetAccount(),
					AmountPaid: tx.GetAmountPaid(),
				})
			}
		}
		return json.Marshal(entities)
	default:
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}
}

func (p *AvgByTypeProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	state := p.getOrCreateClientState(clientID)

	switch entityID {
	case "period1":
		var entities []period1Entity
		if err := json.Unmarshal(data, &entities); err != nil {
			return err
		}
		state.period1Stats = make(map[string]*AvgByTypeStats)
		for _, e := range entities {
			state.period1Stats[e.Format] = &AvgByTypeStats{
				Sum:   e.Sum,
				Count: e.Count,
			}
		}
	case "period2":
		var entities []period2Entity
		if err := json.Unmarshal(data, &entities); err != nil {
			return err
		}
		state.period2Transactions = make(map[string][]*protobuf.AvgByTypeTransaction)
		for _, e := range entities {
			tx := &protobuf.AvgByTypeTransaction{
				Account:    e.Account,
				AmountPaid: e.AmountPaid,
			}
			state.period2Transactions[e.Format] = append(state.period2Transactions[e.Format], tx)
		}
	default:
		return fmt.Errorf("unknown entity: %s", entityID)
	}
	return nil
}

func (p *AvgByTypeProcessor) ClearClientState(clientID string) error {
	return p.Cleanup(clientID)
}
