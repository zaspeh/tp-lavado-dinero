package factory

import (
	hypervisor "github.com/zaspeh/tp-lavado-dinero/internal/fault_hypervisor/fault_hypervisor"
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

	config := hypervisor.FaultHypervisorConfig{
		ConnectionSettings:      connSettings,
		HeartbeatQueueName:      heartbeatQueueName,
		CheckIntervalSeconds:    checkIntervalSeconds,
		HeartbeatTimeoutSeconds: heartbeatTimeoutSeconds,
	}

	return hypervisor.NewFaultHypervisor(config)
}
