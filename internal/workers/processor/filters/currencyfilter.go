package filterprocessor

import protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"

type CurrencyFilterProcessor struct {
	currencyToFilter string
}

func NewCurrencyFilterProcessor(currencyToFilter string) *CurrencyFilterProcessor {
	return &CurrencyFilterProcessor{
		currencyToFilter: currencyToFilter,
	}
}

func (f *CurrencyFilterProcessor) Process(clientID string, msg *protobuf.Transaction) ([]*protobuf.Transaction, bool, error) {
	paymentCurrency := msg.GetPaymentCurrency()
	if paymentCurrency == f.currencyToFilter {
		return []*protobuf.Transaction{msg}, false, nil
	}
	return nil, false, nil
}
