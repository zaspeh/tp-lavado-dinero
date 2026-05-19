package maxbank

type Record struct {
	Account      string
	AmountValue  float64
	AmountString string
}

type ProcessedRecord struct {
	BankName     string
	Account      string
	AmountValue  float64
	AmountString string
}
