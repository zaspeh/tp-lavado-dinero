package filterprocessor

type Amountable interface {
	GetAmount() float64
}

type AmountFilterProcessor[T Amountable] struct {
	AmountToFilter float64
}

func NewAmountFilterProcessor[T Amountable](amount float64) *AmountFilterProcessor[T] {
	return &AmountFilterProcessor[T]{
		AmountToFilter: amount,
	}
}

func (p *AmountFilterProcessor[T]) Process(clientID string, item T) ([]T, bool, error) {
	if item.GetAmount() >= p.AmountToFilter {
		return nil, false, nil
	}

	return []T{item}, false, nil
}
