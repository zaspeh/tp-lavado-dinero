package maxbank

type Record struct {
	Account      string
	AmountValue  float64
	AmountString string
}

type ProcessedRecord struct {
	BankID       int32
	BankName     string
	Account      string
	AmountString string
}
