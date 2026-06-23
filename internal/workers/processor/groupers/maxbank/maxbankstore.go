package maxbank

const (
	defaultBankName = "Unknown Bank"
)

type MaxBankStore struct {
	bankNames       map[int32]string
	maxTransactions map[int32]Record
}

func NewBankStore() *MaxBankStore {
	return &MaxBankStore{
		bankNames:       make(map[int32]string),
		maxTransactions: make(map[int32]Record),
	}
}

func (s *MaxBankStore) UpdateBankName(bankID int32, bankName string) bool {
	if current, ok := s.bankNames[bankID]; ok && current == bankName {
		return false
	}
	s.bankNames[bankID] = bankName
	return true
}

func (s *MaxBankStore) SetBankName(bankID int32, bankName string) {
	s.bankNames[bankID] = bankName
}

func (s *MaxBankStore) BankName(id int32) string {
	if name, ok := s.bankNames[id]; ok {
		return name
	}
	return defaultBankName
}

func (s *MaxBankStore) UpdateMaxTransaction(bankID int32, account string, amount float64, amountStr string) bool {
	current, ok := s.maxTransactions[bankID]

	// Nuevo maximo
	if !ok || amount > current.AmountValue {
		s.maxTransactions[bankID] = Record{
			Account:      account,
			AmountValue:  amount,
			AmountString: amountStr,
		}
		return true
	}
	return false
}

func (s *MaxBankStore) Reader() *Reader {
	return newReader(s)
}

func (s *MaxBankStore) Clear() {
	clear(s.maxTransactions)
	clear(s.bankNames)
}

func (s *MaxBankStore) SetMaxTransaction(bankID int32, account string, amount float64, amountStr string) {
	s.maxTransactions[bankID] = Record{
		Account:      account,
		AmountValue:  amount,
		AmountString: amountStr,
	}
}
