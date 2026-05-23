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

type TransactionBatch []Transaction

func (tb TransactionBatch) Handle(handler MessageHandler) error {
	return handler.HandleTransactionBatch(tb)
}

// Implementacion Naive, para respetar la firma de Wrapper
func NewTransactionBatch(records []Transaction) TransactionBatch {
	return TransactionBatch(records)
}
