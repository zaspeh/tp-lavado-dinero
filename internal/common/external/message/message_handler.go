package message

type MessageHandler interface {
	HandleTransaction(msg Transaction) error
	HandleEOF(msg EOF) error
}
