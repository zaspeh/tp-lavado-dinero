package origin

type Account struct {
	Bank    int32
	Account string
}

func (a Account) GetBank() int32 {
	return a.Bank
}

func (a Account) GetAccount() string {
	return a.Account
}
