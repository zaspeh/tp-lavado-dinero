package maxbank

import (
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

type MaxBankWorker struct {
	maxBankStores map[string]*MaxBankStore
}

func NewMaxBankWorker() *MaxBankWorker {
	return &MaxBankWorker{
		maxBankStores: make(map[string]*MaxBankStore),
	}
}

func (w *MaxBankWorker) Process(clientID string, maxBankMsg *protobuf.MaxBank) error {
	store := w.getStore(clientID)
	bankID := maxBankMsg.GetFromBank()

	if meta := maxBankMsg.GetBankMetadata(); meta != nil {
		store.UpdateBankName(bankID, meta.GetBankName())
		return nil
	}

	if ts := maxBankMsg.GetTransferSummary(); ts != nil {
		amountStr := ts.GetAmount()
		amountVal, err := strconv.ParseFloat(amountStr, 64)
		if err != nil {
			return err
		}

		store.UpdateMaxTransaction(bankID, ts.GetAccount(), amountVal, amountStr)
	}

	return nil
}

func (w *MaxBankWorker) Finalize(clientID string, yield func(result *protobuf.MaxBankResult) error) error {
	store := w.getStore(clientID)
	reader := store.Reader()

	for reader.HasNext() {
		processedRecord := reader.Get()
		maxBankResult := &protobuf.MaxBankResult{
			BankName: processedRecord.BankName,
			Account:  processedRecord.Account,
			Amount:   processedRecord.AmountString,
		}

		if err := yield(maxBankResult); err != nil {
			return err
		}
		reader.Next()
	}

	store.Clear()
	delete(w.maxBankStores, clientID)
	return nil
}

func (w *MaxBankWorker) Cleanup(clientID string) error {
	store := w.getStore(clientID)
	store.Clear()
	delete(w.maxBankStores, clientID)
	return nil
}

func (w *MaxBankWorker) getStore(clientID string) *MaxBankStore {
	if store, ok := w.maxBankStores[clientID]; ok {
		return store
	}

	store := NewBankStore()
	w.maxBankStores[clientID] = store
	return store
}
