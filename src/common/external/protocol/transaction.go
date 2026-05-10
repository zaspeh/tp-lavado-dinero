package protocol

type Transaction struct {
	BankOrigin         string
	AccountOrigin      string
	BankDestination    string
	AccountDestination string
	Amount             float64
	Currency           string
	PaymentType        string
	Timestamp          int64
}
