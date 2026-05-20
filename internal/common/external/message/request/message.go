package request

type Message interface {
	Handle(handler MessageHandler) error
}
