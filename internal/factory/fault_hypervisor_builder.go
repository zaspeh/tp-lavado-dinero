package factory

import (
	configloader "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/config_loader"
	hypervisor "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/fault_hypervisor"
	runtimepkg "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/runtime"
)

func BuildFaultHypervisor() (*hypervisor.FaultHypervisor, error) {
	connSettings, err := getMomConfigFromEnv()
	if err != nil {
		return nil, err
	}

	heartbeatQueueName, err := getEnvStrict("HEARTBEAT_QUEUE_NAME")
	if err != nil {
		return nil, err
	}

	checkIntervalSeconds, err := getEnvIntStrict("CHECK_INTERVAL_SECONDS")
	if err != nil {
		return nil, err
	}

	heartbeatTimeoutSeconds, err := getEnvIntStrict("HEARTBEAT_TIMEOUT_SECONDS")
	if err != nil {
		return nil, err
	}

	loadedConfig, err := configloader.LoadRuntimeConfig("/app/config.yml")
	if err != nil {
		return nil, err
	}

	runtimeConfig := runtimepkg.RuntimeConfig{
		NetworkName:       loadedConfig.NetworkName,
		WorkerImage:       loadedConfig.WorkerImage,
		WorkerDockerfile:  loadedConfig.WorkerDockerfile,
		BuildContext:      loadedConfig.BuildContext,
		MomPort:           loadedConfig.MomPort,
		HeartbeatInterval: loadedConfig.HeartbeatIntervalSeconds,
	}

	config := hypervisor.FaultHypervisorConfig{
		ConnectionSettings:      connSettings,
		HeartbeatQueueName:      heartbeatQueueName,
		CheckIntervalSeconds:    checkIntervalSeconds,
		HeartbeatTimeoutSeconds: heartbeatTimeoutSeconds,
		RuntimeConfig:           runtimeConfig,
	}

	return hypervisor.NewFaultHypervisor(config)
}
