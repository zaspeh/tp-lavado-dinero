package currencyconverter

import "errors"

var ErrorCurrencyNotFound = errors.New("Currency not found")

type Converter interface {
	ConvertToUSD(currencyName string, amount float64) (float64, error)
}
