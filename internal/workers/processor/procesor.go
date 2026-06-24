package processor

import (
	checkpoint "github.com/zaspeh/tp-lavado-dinero/internal/workers/checkpoint"
)

type Processor[T, V any] interface {
	Process(clientID string, item T) ([]V, bool, error)
}

type StatefulProcessor[T, V any] interface {
	Process(clientID string, item T, cm *checkpoint.CheckpointManager) error
	Finalize(clientID string, yield func(result V) error) (uint64, error)
	Cleanup(clientID string) error
}
