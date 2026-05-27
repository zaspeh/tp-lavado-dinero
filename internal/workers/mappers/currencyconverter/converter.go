package currencyconverter

import (
	"errors"
	"time"
)

var ErrorCurrencyNotFound = errors.New("Currency not found")

type Converter interface {
	ConvertToUSD(currencyName string, amount float64, timestamp time.Time) (float64, error)
}
