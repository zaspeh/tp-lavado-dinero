package maxbank

const (
	maxIndex        = 0
	defaultBankName = "Unknown Bank"
	noBankName      = ""
)

type MaxBankRecord struct {
	Account      string
	AmountValue  float64
	AmountString string
	BankName     string
}

type MaxBankStore struct {
	bankNames       map[string]string
	maxTransactions map[string][]MaxBankRecord
}

func NewBankStore() *MaxBankStore {
	return &MaxBankStore{
		bankNames:       make(map[string]string),
		maxTransactions: make(map[string][]MaxBankRecord),
	}
}

func (s *MaxBankStore) UpdateBankName(bankID string, bankName string) {
	s.bankNames[bankID] = bankName
}

func (s *MaxBankStore) UpdateMaxTransaction(bankID string, account string, amount float64, amountStr string) {
	current, ok := s.maxTransactions[bankID]

	// Nuevo maximo
	if !ok || amount > current[maxIndex].AmountValue {
		s.maxTransactions[bankID] = []MaxBankRecord{{
			Account:      account,
			AmountValue:  amount,
			AmountString: amountStr,
		}}
		return
	}

	// Maximo repetido
	if amount == current[maxIndex].AmountValue {
		s.maxTransactions[bankID] = append(s.maxTransactions[bankID], MaxBankRecord{
			Account:      account,
			AmountValue:  amount,
			AmountString: amountStr,
		})
	}
}

func (s *MaxBankStore) GetResults() []MaxBankRecord {
	var results []MaxBankRecord
	for bankID, records := range s.maxTransactions {
		name := s.bankNames[bankID]
		if name == noBankName {
			name = defaultBankName
		}

		for _, r := range records {
			r.BankName = name
			results = append(results, r)
		}
	}
	return results
}
