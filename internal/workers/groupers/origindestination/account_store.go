package origindestination

type AccountStore struct {
	data map[Account]map[Account]struct{}
}

func newAccountStore() *AccountStore {
	return &AccountStore{
		data: make(map[Account]map[Account]struct{}),
	}
}

func (as *AccountStore) Add(account1 Account, account2 Account) {
	if _, exists := as.data[account1]; !exists {
		as.data[account1] = make(map[Account]struct{})
	}

	as.data[account1][account2] = struct{}{}
}

func (as *AccountStore) GetData() map[Account]map[Account]struct{} {
	return as.data
}
