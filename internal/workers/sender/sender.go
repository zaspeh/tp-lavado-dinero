package sender

import m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

type SerializerFunc[V any] func(clientID string, batchID string, batch V) (m.Message, error)

type Sender[V any] interface {
	Add(clientID string, item V, batchID string) error
	Flush(clientID string) error
	Cleanup(clientID string) error
	SendEOF(clientID string, survivorCount uint64) error
	Close() error
}
