package gateway

import (
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/clientregistry"

	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
)

type GatewayConfig struct {
	ServerHost string
	ServerPort string

	MomHost string
	MomPort int

	USDQueueName    string
	OutputQueueName string
}

type Gateway struct {
	config GatewayConfig

	registry clientregistry.ClientRegistry

	listener net.Listener

	usdQueue    m.Middleware
	outputQueue m.Middleware

	running atomic.Bool
}

func New(config GatewayConfig) (*Gateway, error) {
	connSettings := m.ConnSettings{
		Hostname: config.MomHost,
		Port:     config.MomPort,
	}

	usdQueue, err := m.CreateQueueMiddleware(config.USDQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	outputQueue, err := m.CreateQueueMiddleware(config.OutputQueueName, connSettings)
	if err != nil {
		usdQueue.Close()
		return nil, err
	}

	listener, err := net.Listen("tcp", config.ServerHost+":"+config.ServerPort)
	if err != nil {
		usdQueue.Close()
		outputQueue.Close()
		return nil, err
	}

	gateway := &Gateway{
		config:      config,
		registry:    clientregistry.New(),
		listener:    listener,
		usdQueue:    usdQueue,
		outputQueue: outputQueue,
	}

	gateway.running.Store(true)

	return gateway, nil
}

func (gateway *Gateway) Run() error {
	defer g.listener.Close()

	go gateway.outputQueue.StartConsuming(func(msg middleware.Message, ack, nack func()) {
		gateway.handleClientResponse(msg, ack, nack)
	})

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

		go gateway.handleClientRequest(conn)
	}

	gateway.outputQueue.StopConsuming()
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

func (gateway *Gateway) handleClientRequest(conn net.Conn) {
loop:
	for {
		msg, err := external.ReadMessage(conn)
		if err != nil {
			slog.Debug("while reading message", "err", err)
			return
		}

		gateway.registry.Add(msg.JobID, conn)

		switch msg.Type {
		case protocol.TypeTransaction:
			if err := gateway.handleTransactionMessage(msg); err != nil {
				slog.Debug("while handling transaction message", "err", err)
				return
			}

		case protocol.TypeEOF:
			if err := gateway.handleEOFMessage(msg); err != nil {
				slog.Debug("while handling EOF message", "err", err)
				return
			}

			break loop

		default:
			slog.Debug("unexpected message type")
			return
		}
	}
}

func (gateway *Gateway) handleClientResponse(
	msg m.Message,
	ack func(),
	nack func(),
) {
	protocolMsg, err := serializer.DeserializeMessage(msg.Body)

	if err != nil {
		slog.Debug("while deserializing output message", "err", err)
		nack()
		return
	}

	conn, ok := gateway.registry.Get(protocolMsg.JobID)
	if !ok {
		slog.Warn("client connection not found", "jobID", protocolMsg.JobID)
		nack()
		return
	}

	if err := external.WriteMessage(conn, protocolMsg); err != nil {
		slog.Debug("while writing message to client", "err", err)
		nack()
		return
	}

	gateway.registry.Remove(protocolMsg.JobID)

	ack()
}

func (gateway *Gateway) handleTransactionMessage(msg *protocol.Message) error {
	data, err := serializer.SerializeMessage(msg)
	if err != nil {
		slog.Debug("while serializing transaction message", "err", err)
		return err
	}

	rabbitMsg := m.Message{Body: data}

	if err := gateway.usdQueue.Send(rabbitMsg); err != nil {
		slog.Debug("while sending transaction message", "err", err)
		return err
	}

	return nil
}

func (gateway *Gateway) handleEOFMessage(msg *protocol.Message) error {
	slog.Info("received EOF message")

	data, err := serializer.SerializeMessage(msg)
	if err != nil {
		slog.Debug("while serializing EOF message", "err", err)
		return err
	}

	rabbitMsg := m.Message{Body: data}

	if err := gateway.usdQueue.Send(rabbitMsg); err != nil {
		slog.Debug("while sending EOF message", "err", err)
		return err
	}

	return nil
}
