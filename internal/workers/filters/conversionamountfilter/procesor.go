package conversionamountfilter

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"

type Processor struct {
	AmountToFilter float64
}

func New(amountToFilter float64) *Processor {
	return &Processor{
		AmountToFilter: amountToFilter,
	}
}

func (p *Processor) Process(clientID string, item *protobuf.ConvertedAmount) ([]*protobuf.ConvertedAmount, error) {
	if item.Amount >= p.AmountToFilter {
		return nil, nil
	}
	return []*protobuf.ConvertedAmount{item}, nil
}
