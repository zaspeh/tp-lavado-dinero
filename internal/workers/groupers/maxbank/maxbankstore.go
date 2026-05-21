package maxbank

const (
	maxIndex        = 0
	defaultBankName = "Unknown Bank"
)

type MaxBankStore struct {
	bankNames       map[int32]string
	maxTransactions map[int32][]Record
}

func NewBankStore() *MaxBankStore {
	return &MaxBankStore{
		bankNames:       make(map[int32]string),
		maxTransactions: make(map[int32][]Record),
	}
}

func (s *MaxBankStore) UpdateBankName(bankID int32, bankName string) {
	s.bankNames[bankID] = bankName
}

func (s *MaxBankStore) getBankName(id int32) string {
	if name, ok := s.bankNames[id]; ok {
		return name
	}
	return defaultBankName
}

func (s *MaxBankStore) UpdateMaxTransaction(bankID int32, account string, amount float64, amountStr string) {
	current, ok := s.maxTransactions[bankID]

	// Nuevo maximo
	if !ok || amount > current[maxIndex].AmountValue {
		s.maxTransactions[bankID] = []Record{{
			Account:      account,
			AmountValue:  amount,
			AmountString: amountStr,
		}}
		return
	}

	// Maximo repetido
	if amount == current[maxIndex].AmountValue {
		s.maxTransactions[bankID] = append(s.maxTransactions[bankID], Record{
			Account:      account,
			AmountValue:  amount,
			AmountString: amountStr,
		})
	}
}

func (s *MaxBankStore) Reader() *Reader {
	return newReader(s)
}

func (s *MaxBankStore) Flush(bankID int32) {
	delete(s.maxTransactions, bankID)
	delete(s.bankNames, bankID)
}
