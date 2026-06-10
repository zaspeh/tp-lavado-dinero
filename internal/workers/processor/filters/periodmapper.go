package filterprocessor

import (
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/workers/filters/periodfilter"
)

type PeriodMapperRule[T, V any] struct {
	Period periodfilter.Period
	Map    func(T) V
}

type PeriodMapperProcessor[T, V any] struct {
	rules []PeriodMapperRule[T, V]
	time  func(T) time.Time
}

func NewPeriodMapperProcessor[T, V any](
	timeExtractor func(T) time.Time,
	rules ...PeriodMapperRule[T, V],
) *PeriodMapperProcessor[T, V] {
	return &PeriodMapperProcessor[T, V]{
		rules: rules,
		time:  timeExtractor,
	}
}

func (p *PeriodMapperProcessor[T, V]) Process(_ string, item T) ([]V, error) {
	timestamp := p.time(item)
	for _, rule := range p.rules {
		if rule.Period.Contains(timestamp) {
			return []V{rule.Map(item)}, nil
		}
	}
	return nil, nil
}
