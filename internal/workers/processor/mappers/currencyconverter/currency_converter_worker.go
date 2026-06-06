package currencyconverter

import (
	"strconv"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type CurrencyConverterProcessor struct {
	converter Converter
}

func NewCurrencyConverterProcessor(converter Converter) *CurrencyConverterProcessor {
	return &CurrencyConverterProcessor{
		converter: converter,
	}
}

func (w *CurrencyConverterProcessor) Process(clientID string, toConvertMsg *protobuf.ToConvertTypeFilteredPayment) ([]*protobuf.ConvertedAmount, error) {
	currency := toConvertMsg.GetPaymentCurrency()
	amount, err := strconv.ParseFloat(toConvertMsg.GetAmountPaid(), 64)
	if err != nil {
		return nil, err
	}

	timestamp := toConvertMsg.GetTimestamp()
	convertedAmount, err := w.converter.ConvertToUSD(currency, amount, timestamp.AsTime())

	if err == ErrorCurrencyNotFound {
		return nil, nil
	}

	if err != nil {
		return nil, err
	}

	convertedAmountMsg := &protobuf.ConvertedAmount{Amount: convertedAmount}
	return []*protobuf.ConvertedAmount{convertedAmountMsg}, nil
}
