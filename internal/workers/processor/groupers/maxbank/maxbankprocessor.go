package maxbank

import (
	"encoding/json"
	"fmt"
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MaxBankProcessor struct {
	maxBankStores map[string]*MaxBankStore
}

type bankNameEntity struct {
	BankID   int32  `json:"bankId"`
	BankName string `json:"bankName"`
}

type maxTxEntity struct {
	BankID       int32   `json:"bankId"`
	Account      string  `json:"account"`
	AmountValue  float64 `json:"amountValue"`
	AmountString string  `json:"amountString"`
}

func NewMaxBankProcessor() *MaxBankProcessor {
	return &MaxBankProcessor{
		maxBankStores: make(map[string]*MaxBankStore),
	}
}

func (w *MaxBankProcessor) Process(clientID string, maxBankMsg *protobuf.MaxBank, cm *checkpoint.CheckpointManager) error {
	store := w.getStore(clientID)
	bankID := maxBankMsg.GetFromBank()

	if meta := maxBankMsg.GetBankMetadata(); meta != nil {
		store.UpdateBankName(bankID, meta.GetBankName())
		if cm != nil {
			cm.NotifyEntityChanged(clientID, "bankNames")
		}
		return nil
	}

	if ts := maxBankMsg.GetTransferSummary(); ts != nil {
		amountStr := ts.GetAmount()
		amountVal, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return err
		}

		store.UpdateMaxTransaction(bankID, ts.GetAccount(), amountVal, amountStr)
		if cm != nil {
			cm.NotifyEntityChanged(clientID, "maxTx")
		}
	}

	return nil
}

func (w *MaxBankProcessor) Finalize(clientID string, yield func(result *protobuf.MaxBankResult) error) (uint64, error) {
	store := w.getStore(clientID)
	reader := store.Reader()

	var totalGroups uint64

	for reader.HasNext() {
		processedRecord := reader.Get()
		maxBankResult := &protobuf.MaxBankResult{
			BankName: processedRecord.BankName,
			Account:  processedRecord.Account,
			Amount:   processedRecord.AmountString,
		}

		if err := yield(maxBankResult); err != nil {
			return 0, err
		}
		totalGroups++
		reader.Next()
	}

	store.Clear()
	delete(w.maxBankStores, clientID)
	return totalGroups, nil
}

func (w *MaxBankProcessor) Cleanup(clientID string) error {
	store := w.getStore(clientID)
	store.Clear()
	delete(w.maxBankStores, clientID)
	return nil
}

func (w *MaxBankProcessor) getStore(clientID string) *MaxBankStore {
	if store, ok := w.maxBankStores[clientID]; ok {
		return store
	}

	store := NewBankStore()
	w.maxBankStores[clientID] = store
	return store
}

func (w *MaxBankProcessor) ListEntities(clientID string) ([]string, error) {
	store, ok := w.maxBankStores[clientID]
	if !ok {
		return nil, nil
	}

	entities := make([]string, 0, 2)
	if len(store.bankNames) > 0 {
		entities = append(entities, "bankNames")
	}
	if len(store.maxTransactions) > 0 {
		entities = append(entities, "maxTx")
	}
	return entities, nil
}

func (w *MaxBankProcessor) SerializeEntity(clientID, entityID string) ([]byte, error) {
	store, ok := w.maxBankStores[clientID]
	if !ok {
		return nil, fmt.Errorf("store not found for client: %s", clientID)
	}

	switch entityID {
	case "bankNames":
		entities := make([]bankNameEntity, 0, len(store.bankNames))
		for bankID, bankName := range store.bankNames {
			entities = append(entities, bankNameEntity{
				BankID:   bankID,
				BankName: bankName,
			})
		}
		return json.Marshal(entities)
	case "maxTx":
		entities := make([]maxTxEntity, 0, len(store.maxTransactions))
		for bankID, record := range store.maxTransactions {
			entities = append(entities, maxTxEntity{
				BankID:       bankID,
				Account:      record.Account,
				AmountValue:  record.AmountValue,
				AmountString: record.AmountString,
			})
		}
		return json.Marshal(entities)
	default:
		return nil, fmt.Errorf("unknown entity: %s", entityID)
	}
}

func (w *MaxBankProcessor) LoadEntity(clientID, entityID string, data []byte) error {
	store := w.getStore(clientID)

	switch entityID {
	case "bankNames":
		var entities []bankNameEntity
		if err := json.Unmarshal(data, &entities); err != nil {
			return err
		}
		for _, e := range entities {
			store.UpdateBankName(e.BankID, e.BankName)
		}
	case "maxTx":
		var entities []maxTxEntity
		if err := json.Unmarshal(data, &entities); err != nil {
			return err
		}
		for _, e := range entities {
			store.SetMaxTransaction(e.BankID, e.Account, e.AmountValue, e.AmountString)
		}
	default:
		return fmt.Errorf("unknown entity: %s", entityID)
	}
	return nil
}

func (w *MaxBankProcessor) ClearClientState(clientID string) error {
	return w.Cleanup(clientID)
}
