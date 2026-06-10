package runtime

import (
	"fmt"
	"os/exec"

	configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"
)

type DockerRuntime struct{}

func NewDockerRuntime() *DockerRuntime {
	return &DockerRuntime{}
}

func (r *DockerRuntime) CreateWorker(containerName string, workerID int, definition configloader.WorkerDefinition) error {

	args := []string{
		"run",
		"-d",

		"--name",
		containerName,

		"--network",
		"money_laundering_network",

		"-e", "HEARTBEAT_QUEUE_NAME=heartbeat_queue",
		"-e", "HEARTBEAT_INTERVAL_SECONDS=5",

		"-e", fmt.Sprintf("ID=%d", workerID),
		"-e", fmt.Sprintf("CONTAINER_NAME=%s", containerName),
		"-e", fmt.Sprintf("WORKER_TYPE=%s", definition.WorkerType),

		"-e", "MOM_HOST=172.19.0.2",
		"-e", "MOM_PORT=5672",

		"-e", fmt.Sprintf("WORKER_COUNT=%d", definition.Count),
		"-e", fmt.Sprintf("WORKER_EXCHANGE_NAME=%s", definition.WorkerExchangeName),
	}

	for key, value := range definition.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", key, value))
	}

	args = append(args, "tp-worker")

	cmd := exec.Command("docker", args...)

	output, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf(
			"create %s failed: %w (%s)",
			containerName,
			err,
			string(output),
		)
	}

	return nil
}

func (r *DockerRuntime) ContainerExists(containerName string) (bool, error) {
	cmd := exec.Command("docker", "inspect", containerName)

	err := cmd.Run()

	if err == nil {
		return true, nil
	}

	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}

	return false, err
}

func (r *DockerRuntime) RestartWorker(containerName string) error {
	cmd := exec.Command("docker", "restart", containerName)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("restart %s failed: %w (%s)", containerName, err, string(output))
	}

	return nil
}

func (r *DockerRuntime) BuildWorkerImage() error {
	cmd := exec.Command(
		"docker",
		"build",
		"-t",
		"tp-worker",
		"-f",
		"/workspace/cmd/worker/Dockerfile",
		"/workspace",
	)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("build worker image failed: %w (%s)", err, string(output))
	}

	return nil
}

func (r *DockerRuntime) ImageExists(imageName string) (bool, error) {
	cmd := exec.Command("docker", "image", "inspect", imageName)

	err := cmd.Run()

	if err == nil {
		return true, nil
	}

	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}

	return false, err
}

func (r *DockerRuntime) EnsureNetwork(networkName string) error {

	cmd := exec.Command("docker", "network", "inspect", networkName)

	if err := cmd.Run(); err == nil {
		return nil
	}

	cmd = exec.Command("docker", "network", "create", networkName)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("create network failed: %w (%s)", err, string(output))
	}

	return nil
}
