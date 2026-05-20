package result

type MaxBankResult struct {
	BankName string
	Account  string
	Amount   string
}

func (m MaxBankResult) Handle(handler ResultHandler) error {
	return handler.HandleMaxBankResult(m)
}
