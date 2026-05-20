package result

type Result interface {
	Handle(handler ResultHandler) error
}
