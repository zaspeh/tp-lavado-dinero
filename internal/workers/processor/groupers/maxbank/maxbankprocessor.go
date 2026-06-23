package maxbank

import (
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type MaxBankProcessor struct {
	maxBankStores map[string]*MaxBankStore
	tracker       *MaxBankCheckpointTracker
}

func NewMaxBankProcessor() *MaxBankProcessor {
	processor := &MaxBankProcessor{
		maxBankStores: make(map[string]*MaxBankStore),
	}
	processor.tracker = NewMaxBankCheckpointTracker(processor.getStore)
	return processor
}

func (w *MaxBankProcessor) Process(clientID string, maxBankMsg *protobuf.MaxBank, cm *checkpoint.CheckpointManager) error {
	store := w.getStore(clientID)
	bankID := maxBankMsg.GetFromBank()

	if meta := maxBankMsg.GetBankMetadata(); meta != nil {
		bankName := meta.GetBankName()
		if store.UpdateBankName(bankID, bankName) {
			w.tracker.MarkBankName(clientID, bankID, bankName)
		}
		return nil
	}

	if ts := maxBankMsg.GetTransferSummary(); ts != nil {
		amountStr := ts.GetAmount()
		amountVal, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return err
		}

		if store.UpdateMaxTransaction(bankID, ts.GetAccount(), amountVal, amountStr) {
			w.tracker.MarkMaxTransaction(clientID, bankID, Record{
				Account:      ts.GetAccount(),
				AmountValue:  amountVal,
				AmountString: amountStr,
			})
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
	w.tracker.ClearClient(clientID)
	delete(w.maxBankStores, clientID)
	return totalGroups, nil
}

func (w *MaxBankProcessor) Cleanup(clientID string) error {
	store := w.getStore(clientID)
	store.Clear()
	w.tracker.ClearClient(clientID)
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

func (w *MaxBankProcessor) ClearClientState(clientID string) error {
	return w.Cleanup(clientID)
}

func (w *MaxBankProcessor) DrainChanges(clientID string) ([]checkpoint.CheckpointChange, error) {
	return w.tracker.DrainChanges(clientID)
}

func (w *MaxBankProcessor) RestoreChanges(clientID string, changes []checkpoint.CheckpointChange) error {
	return w.tracker.RestoreChanges(clientID, changes)
}

func (w *MaxBankProcessor) ApplyChange(clientID string, change checkpoint.CheckpointChange) error {
	return w.tracker.ApplyChange(clientID, change)
}
