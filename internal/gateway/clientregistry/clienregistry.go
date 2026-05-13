package clientregistry

import (
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/clientconnection"
)

type ClientRegistry struct {
	mutex   sync.RWMutex
	clients map[string]*clientconnection.ClientConnection
}

func New() *ClientRegistry {
	return &ClientRegistry{
		clients: make(map[string]*clientconnection.ClientConnection),
	}
}

func (r *ClientRegistry) Add(
	clientId string,
	conn *clientconnection.ClientConnection,
) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	r.clients[clientId] = conn
}

func (r *ClientRegistry) Get(
	clientId string,
) (*clientconnection.ClientConnection, bool) {
	r.mutex.RLock()
	defer r.mutex.RUnlock()

	conn, ok := r.clients[clientId]
	return conn, ok
}

func (r *ClientRegistry) Remove(
	clientId string,
) {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	delete(r.clients, clientId)
}

func (r *ClientRegistry) CloseAll() {
	r.mutex.Lock()
	defer r.mutex.Unlock()

	for _, client := range r.clients {
		client.Close()
	}

	clear(r.clients)
}
