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

	hypervisorID, err := getEnvIntStrict("HYPERVISOR_ID")
	if err != nil {
		return nil, err
	}

	hypervisorCount, err := getEnvIntStrict("HYPERVISOR_COUNT")
	if err != nil {
		return nil, err
	}

	coordinationExchangeName, err := getEnvStrict("COORDINATION_EXCHANGE_NAME")
	if err != nil {
		return nil, err
	}

	coordinationHeartbeatSeconds, err := getEnvIntStrict("COORDINATION_HEARTBEAT_INTERVAL_SECONDS")
	if err != nil {
		return nil, err
	}

	leaderTimeoutSeconds, err := getEnvIntStrict("ELECTION_TIMEOUT_SECONDS")
	if err != nil {
		return nil, err
	}

	electionTimeoutSeconds, err := getEnvIntStrict("ELECTION_TIMEOUT_SECONDS")
	if err != nil {
		return nil, err
	}

	loadedConfig, err := configloader.LoadRuntimeConfig("/app/config.yml")
	if err != nil {
		return nil, err
	}

	runtimeConfig := runtimepkg.RuntimeConfig{
		NetworkName:                 loadedConfig.NetworkName,
		WorkerImage:                 loadedConfig.WorkerImage,
		WorkerDockerfile:            loadedConfig.WorkerDockerfile,
		BuildContext:                loadedConfig.BuildContext,
		MomPort:                     loadedConfig.MomPort,
		HeartbeatInterval:           loadedConfig.HeartbeatIntervalSeconds,
		HeartbeatQueueName:          loadedConfig.HeartbeatQueueName,
		HypervisorWorkerStoragePath: loadedConfig.HypervisorWorkerStoragePath,
		WorkerStoragePath:           loadedConfig.WorkerStoragePath,
	}

	config := hypervisor.FaultHypervisorConfig{
		ConnectionSettings:      connSettings,
		HeartbeatQueueName:      heartbeatQueueName,
		CheckIntervalSeconds:    checkIntervalSeconds,
		HeartbeatTimeoutSeconds: heartbeatTimeoutSeconds,

		HypervisorID:                 hypervisorID,
		HypervisorCount:              hypervisorCount,
		CoordinationExchangeName:     coordinationExchangeName,
		CoordinationHeartbeatSeconds: coordinationHeartbeatSeconds,
		LeaderTimeoutSeconds:         leaderTimeoutSeconds,
		ElectionTimeoutSeconds:       electionTimeoutSeconds,

		RuntimeConfig: runtimeConfig,
	}

	return hypervisor.NewFaultHypervisor(config)
}
