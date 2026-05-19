package periodfilter

import "time"

type Period struct {
	StartDate time.Time
	EndDate   time.Time
}

func (p Period) Contains(t time.Time) bool {
	return !t.Before(p.StartDate) &&
		!t.After(p.EndDate)
}
