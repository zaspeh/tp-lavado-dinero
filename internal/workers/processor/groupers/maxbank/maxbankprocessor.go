package maxbank

import (
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type MaxBankProcessor struct {
	maxBankStores map[string]*MaxBankStore
}

func NewMaxBankProcessor() *MaxBankProcessor {
	return &MaxBankProcessor{
		maxBankStores: make(map[string]*MaxBankStore),
	}
}

func (w *MaxBankProcessor) Process(clientID string, maxBankMsg *protobuf.MaxBank) error {
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

func (w *MaxBankProcessor) Finalize(clientID string, yield func(result *protobuf.MaxBankResult) error) (uint64, error) {
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
			return 0, err
		}
		reader.Next()
	}

	store.Clear()
	delete(w.maxBankStores, clientID)
	return 0, nil
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
