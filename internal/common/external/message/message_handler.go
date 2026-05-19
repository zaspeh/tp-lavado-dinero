package message

type MessageHandler interface {
	HandleTransaction(Transaction) error
	HandleEOF(EOF) error
}
