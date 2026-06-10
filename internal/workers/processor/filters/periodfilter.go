package filterprocessor

import (
	"time"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type Period struct {
	StartDate time.Time
	EndDate   time.Time
}

func (p Period) Contains(t time.Time) bool {
	return !t.Before(p.StartDate) &&
		!t.After(p.EndDate)
}

type ToConvertPeriodFilterProcessor struct {
	period Period
}

func NewToConvertPeriodFilterProcessor(period Period) *ToConvertPeriodFilterProcessor {
	return &ToConvertPeriodFilterProcessor{
		period: period,
	}
}

func (p *ToConvertPeriodFilterProcessor) Process(_ string, item *protobuf.ToConvertTransaction) ([]*protobuf.ToConvertPeriodFiltered, error) {
	if !p.period.Contains(item.GetTimestamp().AsTime()) {
		return nil, nil
	}

	return []*protobuf.ToConvertPeriodFiltered{
		{
			AmountPaid:      item.GetAmountPaid(),
			PaymentCurrency: item.GetPaymentCurrency(),
			PaymentFormat:   item.GetPaymentFormat(),
			Timestamp:       item.GetTimestamp(),
		},
	}, nil
}

// ------------------------------------------------

type ScatterGatherPeriodFilterProcessor struct {
	period Period
}

func NewScatterGatherPeriodFilterProcessor(period Period) *ScatterGatherPeriodFilterProcessor {
	return &ScatterGatherPeriodFilterProcessor{
		period: period,
	}
}

func (p *ScatterGatherPeriodFilterProcessor) Process(_ string, item *protobuf.PeriodFilter) ([]*protobuf.ScatterGather, error) {
	if !p.period.Contains(item.GetTimestamp().AsTime()) {
		return nil, nil
	}

	return []*protobuf.ScatterGather{
		{
			FromBank:  item.GetFromBank(),
			ToBank:    item.GetToBank(),
			Account:   item.GetAccount(),
			ToAccount: item.GetToAccount(),
		},
	}, nil
}

// -----------------------------------------------

type AvgByTypePeriodFilterProcessor struct {
	firstPeriod  Period
	secondPeriod Period
}

func NewAvgByTypePeriodFilterProcessor(firstPeriod, secondPeriod Period) *AvgByTypePeriodFilterProcessor {
	return &AvgByTypePeriodFilterProcessor{
		firstPeriod:  firstPeriod,
		secondPeriod: secondPeriod,
	}
}

func (p *AvgByTypePeriodFilterProcessor) Process(_ string, item *protobuf.PeriodFilter) ([]*protobuf.AvgByTypeTransaction, error) {
	timestamp := item.GetTimestamp().AsTime()
	if p.firstPeriod.Contains(timestamp) {
		return []*protobuf.AvgByTypeTransaction{
			p.buildTransaction(item, protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_FIRST),
		}, nil
	}

	if p.secondPeriod.Contains(timestamp) {
		return []*protobuf.AvgByTypeTransaction{
			p.buildTransaction(item, protobuf.AvgByTypePeriod_AVGBYTYPE_PERIOD_SECOND),
		}, nil
	}

	return nil, nil
}

func (p *AvgByTypePeriodFilterProcessor) buildTransaction(item *protobuf.PeriodFilter, period protobuf.AvgByTypePeriod) *protobuf.AvgByTypeTransaction {
	return &protobuf.AvgByTypeTransaction{
		Account:       item.GetAccount(),
		AmountPaid:    item.GetAmountPaid(),
		PaymentFormat: item.GetPaymentFormat(),
		Period:        period,
	}
}
