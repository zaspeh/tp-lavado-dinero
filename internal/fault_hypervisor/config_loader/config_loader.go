package configloader

import (
	"os"
	"strconv"

	"gopkg.in/yaml.v3"
)

type WorkerDefinition struct {
	ServiceName        string
	WorkerType         string
	WorkerExchangeName string
	Count              int

	MomHost string
	MomPort int

	Env map[string]string
}

type configFile struct {
	RabbitMQ rabbitConfig             `yaml:"rabbitmq"`
	Services map[string]serviceConfig `yaml:"services"`
}

type rabbitConfig struct {
	AmqpPort int `yaml:"amqp_port"`
}

type serviceConfig struct {
	Count              int               `yaml:"count"`
	WorkerType         string            `yaml:"worker_type"`
	WorkerExchangeName string            `yaml:"worker_exchange_name"`
	Env                map[string]string `yaml:"env"`
}

func LoadWorkersFromConfig(path string) ([]WorkerDefinition, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var cfg configFile

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}

	var workers []WorkerDefinition

	for serviceName, service := range cfg.Services {
		if !isWorker(serviceName, service) {
			continue
		}

		exchange := service.WorkerExchangeName

		if exchange == "" {
			exchange = service.WorkerType
		}

		workers = append(workers, WorkerDefinition{
			ServiceName:        serviceName,
			WorkerType:         service.WorkerType,
			Count:              service.Count,
			Env:                service.Env,
			WorkerExchangeName: exchange,
		})
	}

	return workers, nil
}

func isWorker(serviceName string, service serviceConfig) bool {
	if serviceName == "client" {
		return false
	}

	return service.WorkerType != ""
}

func ExpandWorkers(definitions []WorkerDefinition) []string {
	var workers []string

	for _, def := range definitions {
		for i := 0; i < def.Count; i++ {
			workers = append(
				workers,
				def.ServiceName+"_"+strconv.Itoa(i),
			)
		}
	}

	return workers
}
