package clientregistry

import (
	"net"
	"sync"
)

type ClientRegistry struct {
	mutex sync.RWMutex

	clients map[string]net.Conn
}

func New() ClientRegistry {
	return ClientRegistry{
		clients: make(map[string]net.Conn),
	}
}

func (r *ClientRegistry) Add(
	jobID string,
	conn net.Conn,
) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.clients[jobID] = conn
}

func (r *ClientRegistry) Get(
	jobID string,
) (net.Conn, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	conn, ok := r.clients[jobID]

	return conn, ok
}

func (r *ClientRegistry) Remove(
	jobID string,
) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.clients, jobID)
}
