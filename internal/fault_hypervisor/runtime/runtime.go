package runtime

import configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"

type RuntimeConfig struct {
	NetworkName                 string
	WorkerImage                 string
	MomPort                     int
	HeartbeatQueueName          string
	HeartbeatInterval           int
	WorkerDockerfile            string
	BuildContext                string
	HypervisorWorkerStoragePath string
	WorkerStoragePath           string
}

type Runtime interface {
	ContainerExists(containerName string) (bool, error)

	CreateWorker(containerName string, workerID int, definition configloader.WorkerDefinition) error

	RestartWorker(containerName string) error

	BuildWorkerImage() error

	ImageExists() (bool, error)

	EnsureNetwork() error
}
