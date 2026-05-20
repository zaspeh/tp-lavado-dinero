package result

type ResultHandler interface {
	HandleMicrotransactionResult(result MicrotransactionResult) error
	HandleMaxBankResult(result MaxBankResult) error
	HandleEOF(result EOF) error
}
