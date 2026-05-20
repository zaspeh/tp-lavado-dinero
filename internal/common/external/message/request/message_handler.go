package request

type MessageHandler interface {
	HandleTransaction(Transaction) error
	HandleEOF(EOF) error
}
