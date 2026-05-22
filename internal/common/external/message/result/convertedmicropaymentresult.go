package result

type ConvertedMicroPaymentResult struct {
	Count int64
}

func (c ConvertedMicroPaymentResult) Handle(handler ResultHandler) error {
	return handler.HandleConvertedMicroPaymentResult(c)
}
