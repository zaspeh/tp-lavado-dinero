package request

type Transaction struct {
	Record string
}

func NewTransaction(record string) Transaction {
	return Transaction{Record: record}
}

func (t Transaction) Handle(handler MessageHandler) error {
	return handler.HandleTransaction(t)
}

// Implementacion Naive, para respetar la firma de Wrapper
func NewTransactionBatch(records []Transaction) []Transaction {
	return records
}
