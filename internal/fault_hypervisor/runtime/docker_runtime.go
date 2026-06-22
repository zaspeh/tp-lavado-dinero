package runtime

import (
	"fmt"
	"os/exec"
	"strings"

	configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"
)

type DockerRuntime struct {
	config  RuntimeConfig
	gateway string
}

func NewDockerRuntime(config RuntimeConfig) (*DockerRuntime, error) {
	gateway, err := discoverGateway()
	if err != nil {
		return nil, err
	}
	return &DockerRuntime{
		config:  config,
		gateway: gateway,
	}, nil
}

func (r *DockerRuntime) CreateWorker(containerName string, workerID int, definition configloader.WorkerDefinition) error {

	args := []string{
		"run",
		"-d",

		"--name",
		containerName,

		"--network",
		r.config.NetworkName,

		"-e", fmt.Sprintf("HEARTBEAT_QUEUE_NAME=%s", r.config.HeartbeatQueueName),
		"-e", fmt.Sprintf("HEARTBEAT_INTERVAL_SECONDS=%d", r.config.HeartbeatInterval),
		"-e", fmt.Sprintf("ID=%d", workerID),
		"-e", fmt.Sprintf("CONTAINER_NAME=%s", containerName),
		"-e", fmt.Sprintf("WORKER_TYPE=%s", definition.WorkerType),
		"-e", fmt.Sprintf("MOM_HOST=%s", r.gateway),
		"-e", fmt.Sprintf("MOM_PORT=%d", r.config.MomPort),
		"-e", fmt.Sprintf("WORKER_COUNT=%d", definition.Count),
		"-e", fmt.Sprintf("WORKER_EXCHANGE_NAME=%s", definition.WorkerExchangeName),
		"-e", fmt.Sprintf("LOG_LEVEL=%s", definition.LogLevel),
		"-e", fmt.Sprintf("MAX_BATCH_WEIGHT=%d", definition.MaxBatchSize),
	}

	for key, value := range definition.Env {
		args = append(args, "-e", fmt.Sprintf("%s=%s", key, value))
	}

	args = append(args, r.config.WorkerImage)

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
		r.config.WorkerDockerfile,
		r.config.BuildContext,
	)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("build worker image failed: %w (%s)", err, string(output))
	}

	return nil
}

func (r *DockerRuntime) ImageExists() (bool, error) {
	cmd := exec.Command("docker", "image", "inspect", r.config.WorkerImage)

	err := cmd.Run()

	if err == nil {
		return true, nil
	}

	if _, ok := err.(*exec.ExitError); ok {
		return false, nil
	}

	return false, err
}

func (r *DockerRuntime) EnsureNetwork() error {

	cmd := exec.Command("docker", "network", "inspect", r.config.NetworkName)

	if err := cmd.Run(); err == nil {
		return nil
	}

	cmd = exec.Command("docker", "network", "create", r.config.NetworkName)

	output, err := cmd.CombinedOutput()

	if err != nil {
		return fmt.Errorf("create network failed: %w (%s)", err, string(output))
	}

	return nil
}

func discoverGateway() (string, error) {
	cmd := exec.Command("sh", "-c", "ip route | grep default")

	output, err := cmd.Output()
	if err != nil {
		return "", err
	}

	fields := strings.Fields(string(output))

	if len(fields) < 3 {
		return "", fmt.Errorf("unable to parse gateway")
	}

	return fields[2], nil
}
