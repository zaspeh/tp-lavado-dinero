package message

type MessageHandler interface {
	HandleTransaction(msg Message) error
	HandleEOF(msg Message) error
}
