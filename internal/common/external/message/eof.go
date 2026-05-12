package message

type EOF struct{}

func (e EOF) Handle(handler MessageHandler) error {
	return handler.HandleEOF(e)
}
