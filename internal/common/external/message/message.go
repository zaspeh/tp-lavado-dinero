package message

type Message interface {
	Handle(handler MessageHandler) error
}
