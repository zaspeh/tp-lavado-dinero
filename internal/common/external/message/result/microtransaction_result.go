package result

type MicrotransactionResult struct {
	Account   string
	ToAccount string
	Amount    float64
}

func (m MicrotransactionResult) Handle(handler ResultHandler) error {
	return handler.HandleMicrotransactionResult(m)
}
