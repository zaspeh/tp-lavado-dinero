package maxbank

type Record struct {
	Account      string
	AmountValue  float64
	AmountString string
}

type ProcessedRecord struct {
	BankID       string
	BankName     string
	Account      string
	AmountString string
}
