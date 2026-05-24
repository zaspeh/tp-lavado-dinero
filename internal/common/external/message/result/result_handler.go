package result

type ResultHandler interface {
	HandleEOF(msg EOF) error
	HandleMicrotransactionResult(msg MicrotransactionResult) error
	HandleMaxBankResult(msg MaxBankResult) error
	HandleConvertedMicroPaymentResult(msg ConvertedMicroPaymentResult) error
	HandleAvgByTypeResult(msg AvgByTypeResult) error
	HandleSuspiciousAccountsResult(msg SuspiciousAccountsResult) error
}
