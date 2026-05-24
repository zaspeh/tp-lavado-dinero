package result

type SuspiciousAccount struct {
	Bank    int32
	Account string
}

type SuspiciousAccountsResult struct {
	Accounts []SuspiciousAccount
}

func (s SuspiciousAccountsResult) Handle(handler ResultHandler) error {

	return handler.HandleSuspiciousAccountsResult(s)
}
