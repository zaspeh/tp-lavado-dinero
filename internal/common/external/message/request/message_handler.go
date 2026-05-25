package request

type MessageHandler interface {
	HandleTransactionBatch(TransactionBatch) error
	HandleAccountBatch(AccountBatch) error
	HandleEOF(EOF) error
}
