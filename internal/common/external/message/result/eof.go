package result

type EOF struct{}

func (e EOF) Handle(handler ResultHandler) error {
	return handler.HandleEOF(e)
}
