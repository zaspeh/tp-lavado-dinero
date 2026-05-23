package request

type MessageHandler interface {
	HandleTransactionBatch(TransactionBatch) error
	HandleEOF(EOF) error
}
