package workerloading

import (
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type WorkerInfo struct {
	ContainerName string
}

type composeFile struct {
	Services map[string]composeService `yaml:"services"`
}

type composeService struct {
	ContainerName string `yaml:"container_name"`
}

func LoadWorkersFromCompose(path string) ([]WorkerInfo, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var compose composeFile

	if err := yaml.Unmarshal(data, &compose); err != nil {
		return nil, err
	}

	var workers []WorkerInfo

	for _, service := range compose.Services {
		if !isWorker(service.ContainerName) {
			continue
		}

		workers = append(workers, WorkerInfo{ContainerName: service.ContainerName})
	}

	return workers, nil
}

func isWorker(containerName string) bool {
	switch containerName {
	case "rabbitmq":
		return false

	case "gateway":
		return false

	case "fault_hypervisor":
		return false
	}

	if strings.HasPrefix(containerName, "client_") {
		return false
	}

	return true
}
