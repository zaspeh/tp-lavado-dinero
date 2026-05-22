package currencyconverter

import "errors"

var ErrorCurrencyNotFound = errors.New("Currency not found")

type Converter interface {
	GetExchangeRate() map[string]float64
}
