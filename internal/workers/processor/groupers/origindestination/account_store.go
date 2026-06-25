package origindestination

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/model"

type Account = model.Account

type AccountStore struct {
	data map[Account]map[Account]struct{}
}

func newAccountStore() *AccountStore {
	return &AccountStore{
		data: make(map[Account]map[Account]struct{}),
	}
}

func (as *AccountStore) Add(account1 Account, account2 Account) bool {
	if _, exists := as.data[account1]; !exists {
		as.data[account1] = make(map[Account]struct{})
	}

	if _, exists := as.data[account1][account2]; exists {
		return false
	}

	as.data[account1][account2] = struct{}{}
	return true
}

func (as *AccountStore) GetData() map[Account]map[Account]struct{} {
	return as.data
}

func (as *AccountStore) Clear() {
	clear(as.data)
}
