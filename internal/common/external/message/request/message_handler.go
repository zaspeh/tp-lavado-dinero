package request

type MessageHandler interface {
	HandleTransaction(Transaction) error
	HandleTransactionBatch(TransactionBatch) error
	HandleEOF(EOF) error
}
