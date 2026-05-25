package maxbank

type Reader struct {
	storage     *MaxBankStore
	bankIDs     []int32
	currentBank int
}

func newReader(s *MaxBankStore) *Reader {
	ids := make([]int32, 0, len(s.maxTransactions))
	for id := range s.maxTransactions {
		ids = append(ids, id)
	}
	return &Reader{
		storage: s,
		bankIDs: ids,
	}
}

func (r *Reader) HasNext() bool {
	return r.currentBank < len(r.bankIDs)
}

func (r *Reader) Get() ProcessedRecord {
	bankID := r.bankIDs[r.currentBank]
	record := r.storage.maxTransactions[bankID]

	return ProcessedRecord{
		BankID:       bankID,
		BankName:     r.storage.BankName(bankID),
		Account:      record.Account,
		AmountString: record.AmountString,
	}
}

func (r *Reader) Next() {
	r.currentBank++
}
