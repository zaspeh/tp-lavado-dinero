package origin

type AccountStore struct {
	data map[Account]map[Account]struct{}
}
