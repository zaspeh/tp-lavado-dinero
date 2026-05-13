package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/google/uuid"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/clientconnection"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/clientregistry"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
)

type GatewayConfig struct {
	ServerHost         string
	ServerPort         string
	MomHost            string
	MomPort            int
	USDQueueName       string
	ClientExchangeName string
}

type Gateway struct {
	config   GatewayConfig
	registry *clientregistry.ClientRegistry
	listener net.Listener
	running  atomic.Bool
}

func New(config GatewayConfig) (*Gateway, error) {
	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		return nil, err
	}

	gateway := &Gateway{
		config:   config,
		registry: clientregistry.New(),
		listener: listener,
	}

	gateway.running.Store(true)

	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer gateway.listener.Close()
	go gateway.handleSignals()

	slog.Info("accepting connections")
	for {
		conn, err := gateway.listener.Accept()
		if err != nil {
			if !gateway.running.Load() {
				break
			}

			return err
		}
		go gateway.handleIncomingConnection(conn)
	}
	gateway.registry.CloseAll()
	return nil
}

func (gateway *Gateway) handleSignals() {
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("shutdown signal received")
	gateway.running.Store(false)
	gateway.listener.Close()
}

func (gateway *Gateway) handleIncomingConnection(conn net.Conn) {
	connSettings := m.ConnSettings{
		Hostname: gateway.config.MomHost,
		Port:     gateway.config.MomPort,
	}
	socket := socket.New(conn)
	protocol := external.New(socket)
	clientId := gateway.generateClientId()
	client, err := clientconnection.New(clientId, protocol, connSettings, gateway.config.USDQueueName, gateway.config.ClientExchangeName)
	if err != nil {
		slog.Error("failed to create client connection", "error", err)
		protocol.Close()
		return
	}
	gateway.registry.Add(clientId, client)
	if err := client.Run(); err != nil {
		slog.Error("client connection error", "error", err)
	}
	client.Close()
	gateway.registry.Remove(clientId)
}

func (gateway *Gateway) generateClientId() string {
	id := uuid.New().String()
	_, exists := gateway.registry.Get(id)
	if exists {
		return gateway.generateClientId()
	}
	return id
}
