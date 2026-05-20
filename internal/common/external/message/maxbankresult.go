package message

type MaxBankResult struct {
	BankName string
	Account  string
	Amount   string
}

func (m MaxBankResult) IsResult() {}
