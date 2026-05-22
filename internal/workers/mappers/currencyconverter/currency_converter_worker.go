package currencyconverter

type CurrencyConverterConfig struct {
	InputQueueName  string
	OutputQueueName string
	MomHost         string
	MomPort         int
	Converter       Converter
}
