package origin

type AccountStore struct {
	data map[Account]map[Account]struct{}
}

func newAccountStore() *AccountStore {
	return &AccountStore{
		data: make(map[Account]map[Account]struct{}),
	}
}

func (as *AccountStore) Add(origin Account, destination Account) {
	if _, exists := as.data[origin]; !exists {
		as.data[origin] = make(map[Account]struct{})
	}

	as.data[origin][destination] = struct{}{}
}

func (as *AccountStore) GetData() map[Account]map[Account]struct{} {
	return as.data
}
