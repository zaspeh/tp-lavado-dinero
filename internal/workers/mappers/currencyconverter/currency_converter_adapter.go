package currencyconverter

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

type APIExchangeRate struct {
	Quote string  `json:"quote"`
	Rate  float64 `json:"rate"`
}

var isoToLongName = map[string]string{
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

var bitcoinRatesUSD = map[string]float64{
	"2022-09-01": 19793.1,
	"2022-09-02": 199999.0,
	"2022-09-03": 19831.4,
	"2022-09-04": 19952.7,
	"2022-09-05": 20126.1,
}

type CurrencyConverter struct {
	apiURL      string
	ratesByDate map[string]map[string]float64
}

func NewCurrencyConverter(url string) (*CurrencyConverter, error) {
	return &CurrencyConverter{
		apiURL:      url,
		ratesByDate: make(map[string]map[string]float64),
	}, nil
}

func (c *CurrencyConverter) ConvertToUSD(currencyName string, amount float64, timestamp time.Time) (float64, error) {
	dateKey := timestamp.Format("2006-01-02")
	rates, err := c.getRatesForDate(dateKey)
	if err != nil {
		return 0, err
	}

	rate, ok := rates[currencyName]
	if !ok || rate == 0 {
		return 0, ErrorCurrencyNotFound
	}

	return amount / rate, nil
}

func (c *CurrencyConverter) getRatesForDate(dateKey string) (map[string]float64, error) {
	if rates, ok := c.ratesByDate[dateKey]; ok {
		return rates, nil
	}

	url := buildRatesURL(c.apiURL, dateKey)
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected exchange rate API status: %s", resp.Status)
	}

	var apiResponse []APIExchangeRate
	if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
		return nil, err
	}

	finalRates := make(map[string]float64)
	for _, rate := range apiResponse {
		if longName, ok := isoToLongName[rate.Quote]; ok {
			finalRates[longName] = rate.Rate
		}
	}

	finalRates["US Dollar"] = 1.0
	if btcUSD, ok := bitcoinRatesUSD[dateKey]; ok {
		finalRates["Bitcoin"] = 1.0 / btcUSD
	}

	c.ratesByDate[dateKey] = finalRates

	return finalRates, nil
}

func buildRatesURL(baseURL, dateKey string) string {
	if strings.Contains(baseURL, "{date}") {
		return strings.ReplaceAll(baseURL, "{date}", dateKey)
	}
	if strings.Contains(baseURL, "date=") {
		return baseURL
	}
	if strings.Contains(baseURL, "?") {
		return baseURL + "&date=" + dateKey
	}
	return baseURL + "?date=" + dateKey
}
