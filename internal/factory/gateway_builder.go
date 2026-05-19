package factory

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway"
)

func buildGateway() (*gateway.Gateway, error) {
	USDQueueName, err := getEnvStrict("USD_QUEUE")
	if err != nil {
		return nil, err
	}

	outputQueueName, err := getEnvStrict("CLIENT_EXCHANGE")
	if err != nil {
		return nil, err
	}

	serverHost, err := getEnvStrict("SERVER_HOST")
	if err != nil {
		return nil, err
	}

	serverPort, err := getEnvStrict("SERVER_PORT")
	if err != nil {
		return nil, err
	}

	momPort, err := getEnvIntStrict("MOM_PORT")
	if err != nil {
		return nil, err
	}

	momHost, err := getEnvStrict("MOM_HOST")
	if err != nil {
		return nil, err
	}

	config := gateway.GatewayConfig{
		CurrencyQueueName:  USDQueueName,
		ClientExchangeName: outputQueueName,
		ServerHost:         serverHost,
		ServerPort:         serverPort,
		MomHost:            momHost,
		MomPort:            momPort,
	}

	return gateway.New(config)
}
