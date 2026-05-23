package result

type AvgByTypeResult struct {
	Account    string
	AmountPaid string
}

func (r AvgByTypeResult) Handle(handler ResultHandler) error {
	return handler.HandleAvgByTypeResult(r)
}
