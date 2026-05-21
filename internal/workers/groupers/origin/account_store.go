package origin

type AccountStore struct {
	data map[Account]map[Account]struct{}
}

func newAccountStore() *AccountStore {
	return &AccountStore{
		data: make(map[Account]map[Account]struct{}),
	}
}
