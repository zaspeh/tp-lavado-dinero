package runtime

import configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"

type Runtime interface {
	ContainerExists(containerName string) (bool, error)

	CreateWorker(containerName string, workerID int, definition configloader.WorkerDefinition) error

	RestartWorker(containerName string) error

	BuildWorkerImage() error

	ImageExists(imageName string) (bool, error)

	EnsureNetwork(networkName string) error
}
