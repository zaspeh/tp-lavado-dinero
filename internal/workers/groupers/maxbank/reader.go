package maxbank

type Reader struct {
	storage       *MaxBankStore
	bankIDs       []string
	currentBank   int
	currentRecord int
}

func newReader(s *MaxBankStore) *Reader {
	ids := make([]string, 0, len(s.maxTransactions))
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
	records := r.storage.maxTransactions[bankID]

	return ProcessedRecord{
		BankName:     r.storage.getBankName(bankID), // Este es el "join de id"
		Account:      records[r.currentRecord].Account,
		AmountString: records[r.currentRecord].AmountString,
	}
}

func (r *Reader) Next() {
	bankID := r.bankIDs[r.currentBank]
	r.currentRecord++

	if r.currentRecord >= len(r.storage.maxTransactions[bankID]) {
		r.currentBank++
		r.currentRecord = 0
	}
}
