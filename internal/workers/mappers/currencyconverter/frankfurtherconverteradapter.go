package currencyconverter

import (
	"encoding/json"
	"net/http"
)

type APIExchangeRate struct {
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}

type CurrencyConverter struct {
	Rates map[string]float64
}

func NewCurrencyConverter(url string) (*CurrencyConverter, error) {
	isoToLongName := map[string]string{
		"AUD": "Australian Dollar",
		"BRL": "Brazil Real",
		"CAD": "Canadian Dollar",
		"EUR": "Euro",
		"MXN": "Mexican Peso",
		"RUB": "Ruble",
		"INR": "Rupee",
		"SAR": "Saudi Riyal",
		"ILS": "Shekel",
		"CHF": "Swiss Franc",
		"GBP": "UK Pound",
		"JPY": "Yen",
		"CNY": "Yuan",
	}

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var apiRates []APIExchangeRate
	if err := json.NewDecoder(resp.Body).Decode(&apiRates); err != nil {
		return nil, err
	}

	finalRates := make(map[string]float64)
	for _, r := range apiRates {
		if longName, ok := isoToLongName[r.Quote]; ok {
			finalRates[longName] = r.Rate
		}
	}

	// Casos especiales que no estan en la api, ver que hacer con bitcoin
	finalRates["US Dollar"] = 1.0
	finalRates["Bitcoin"] = 78.33

	return &CurrencyConverter{Rates: finalRates}, nil
}

func (c *CurrencyConverter) ConvertToUSD(fullName string, amount float64) (float64, error) {
	rate, ok := c.Rates[fullName]
	if !ok {
		return 0, ErrorCurrencyNotFound
	}

	// TODO: manejar casos donde la tasa de cambio es 0
	return amount / rate, nil
}
