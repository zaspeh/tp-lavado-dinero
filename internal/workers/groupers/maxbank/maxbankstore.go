package maxbank

const (
	maxIndex        = 0
	defaultBankName = "Unknown Bank"
	noBankName      = ""
)

type MaxBankStore struct {
	bankNames       map[string]string
	maxTransactions map[string][]Record
}

func NewBankStore() *MaxBankStore {
	return &MaxBankStore{
		bankNames:       make(map[string]string),
		maxTransactions: make(map[string][]Record),
	}
}

func (s *MaxBankStore) UpdateBankName(bankID string, bankName string) {
	s.bankNames[bankID] = bankName
}

func (s *MaxBankStore) UpdateMaxTransaction(bankID string, account string, amount float64, amountStr string) {
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
	return NewReader(s)
}
