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
	Env                map[string]string
	LogLevel           string
	MaxBatchSize       int
}

type configFile struct {
	GlobalLogLevel        string                   `yaml:"global_log_level"`
	RabbitMQ              rabbitConfig             `yaml:"rabbitmq"`
	FaultHypervisor       faultHypervisorConfig    `yaml:"fault_hypervisor"`
	Services              map[string]serviceConfig `yaml:"services"`
	GlobalWorkerBatchSize int                      `yaml:"global_worker_batch_size"`
}

type rabbitConfig struct {
	AmqpPort int `yaml:"amqp_port"`
}

type ElectionConfig struct {
	ExchangeName             string `yaml:"exchange_name"`
	HeartbeatIntervalSeconds int    `yaml:"heartbeat_interval_seconds"`
	TimeoutSeconds           int    `yaml:"timeout_seconds"`
	ElectionTimeoutSeconds   int    `yaml:"election_timeout_seconds"`
}

type RuntimeConfig struct {
	NetworkName                 string `yaml:"network_name"`
	WorkerImage                 string `yaml:"worker_image"`
	WorkerDockerfile            string `yaml:"worker_dockerfile"`
	BuildContext                string `yaml:"build_context"`
	MomPort                     int    `yaml:"mom_port"`
	HeartbeatIntervalSeconds    int    `yaml:"heartbeat_interval_seconds"`
	HeartbeatQueueName          string `yaml:"heartbeat_queue_name"`
	HypervisorWorkerStoragePath string `yaml:"hypervisor_worker_storage_path"`
	WorkerStoragePath           string `yaml:"worker_storage_path"`

	HypervisorCount              int
	CoordinationExchangeName     string
	CoordinationHeartbeatSeconds int
	LeaderTimeoutSeconds         int
	ElectionTimeoutSeconds       int
}

type faultHypervisorConfig struct {
	Runtime  RuntimeConfig  `yaml:"runtime"`
	Election ElectionConfig `yaml:"election"`
	Count    int            `yaml:"count"`
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
			LogLevel:           cfg.GlobalLogLevel,
			MaxBatchSize:       cfg.GlobalWorkerBatchSize,
		})
	}

	return workers, nil
}

func LoadRuntimeConfig(path string) (RuntimeConfig, error) {

	data, err := os.ReadFile(path)
	if err != nil {
		return RuntimeConfig{}, err
	}

	var cfg configFile

	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return RuntimeConfig{}, err
	}

	return RuntimeConfig{
		NetworkName:                 cfg.FaultHypervisor.Runtime.NetworkName,
		WorkerImage:                 cfg.FaultHypervisor.Runtime.WorkerImage,
		WorkerDockerfile:            cfg.FaultHypervisor.Runtime.WorkerDockerfile,
		BuildContext:                cfg.FaultHypervisor.Runtime.BuildContext,
		MomPort:                     cfg.FaultHypervisor.Runtime.MomPort,
		HeartbeatIntervalSeconds:    cfg.FaultHypervisor.Runtime.HeartbeatIntervalSeconds,
		HeartbeatQueueName:          cfg.FaultHypervisor.Runtime.HeartbeatQueueName,
		HypervisorWorkerStoragePath: cfg.FaultHypervisor.Runtime.HypervisorWorkerStoragePath,
		WorkerStoragePath:           cfg.FaultHypervisor.Runtime.WorkerStoragePath,

		HypervisorCount:              cfg.FaultHypervisor.Count,
		CoordinationExchangeName:     cfg.FaultHypervisor.Election.ExchangeName,
		CoordinationHeartbeatSeconds: cfg.FaultHypervisor.Election.HeartbeatIntervalSeconds,
		LeaderTimeoutSeconds:         cfg.FaultHypervisor.Election.TimeoutSeconds,
		ElectionTimeoutSeconds:       cfg.FaultHypervisor.Election.ElectionTimeoutSeconds,
	}, nil
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
			workers = append(workers, def.ServiceName+"_"+strconv.Itoa(i))
		}
	}

	return workers
}
