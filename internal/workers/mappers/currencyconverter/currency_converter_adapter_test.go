package currencyconverter

import (
	"testing"
)

func TestCurrencyConverter(t *testing.T) {
	url := "https://api.frankfurter.dev/v2/rates?base=USD"
	converter, err := NewCurrencyConverter(url)

	if err != nil {
		t.Fatalf("Could not create converter: %v", err)
	}

	t.Run("Convert base USD", func(t *testing.T) {
		got, err := converter.ConvertToUSD("US Dollar", 50.0)
		if err != nil {
			t.Errorf("Could not convert currency: %v", err)
		}
		if got != 50.0 {
			t.Errorf("Expected 50.0, got %f", got)
		}
	})

	t.Run("Convert to other currency", func(t *testing.T) {

		rateMXN, ok := converter.Rates["Mexican Peso"]
		if !ok {
			t.Fatal("COULD NOT FIND EXCHANGE RATE FOR MXN")
		}

		t.Logf("Actual rate for MXN: %f", rateMXN)

		pesos := 10.0
		usd, err := converter.ConvertToUSD("Mexican Peso", pesos)

		if err != nil {
			t.Errorf("Error to convert: %v", err)
		}

		expectedUSD := pesos / rateMXN
		if usd != expectedUSD {
			t.Errorf("Bad conversion: obtained %f, expected %f", usd, expectedUSD)
		}
	})

	t.Run("Inexistent currency", func(t *testing.T) {
		_, err := converter.ConvertToUSD("Inexistent Currency", 100)
		if err == nil {
			t.Error("Expected an error for an inexistent currency, but none occurred")
		}
	})
}
