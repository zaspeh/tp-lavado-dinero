package client

import (
	"bufio"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/client/storage"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
)

type ClientConfig struct {
	ServerHost string
	ServerPort string
	InputFile  string
	OutputDir  string
}

type Client struct {
	config   ClientConfig
	running  atomic.Bool
	protocol *external.ExternalProtocol
	writer   *storage.ResultCSVWriter
}

func connectWithRetry(host string, port string) (*socket.Socket, error) {
	address := host + ":" + port
	maxAttemps := 3
	var err error
	for i := range maxAttemps {
		conn, err_aux := net.Dial("tcp", address)
		if err_aux == nil {
			return socket.New(conn), nil
		}
		err = err_aux

		// Exponential backoff strategy for reconnection attempts
		waitTime := time.Duration(math.Pow(2, float64(i))) * time.Second
		time.Sleep(waitTime)
	}
	return nil, err
}
func New(config ClientConfig) (*Client, error) {
	socket, err := connectWithRetry(config.ServerHost, config.ServerPort)
	if err != nil {
		return nil, err
	}

	writer := storage.NewResultCSVWriter(config.OutputDir)

	client := &Client{
		config:   config,
		protocol: external.New(socket),
		writer:   writer,
	}

	client.running.Store(true)
	return client, nil
}

func (c *Client) Run() error {
	defer c.protocol.Close()
	go c.handleSignals()

	err := c.processTransactions()
	if err != nil {
		return err
	}

	err = c.receiveResults()
	if err != nil {
		return err
	}

	return nil
}

func (c *Client) processTransactions() error {
	file, err := os.Open(c.config.InputFile)
	if err != nil {
		slog.Debug("Error while runninging input file", "err", err)
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if !c.running.Load() {
			break
		}
		record := scanner.Text()
		transactionMessage := request.NewTransaction(record)
		err := c.protocol.SendTransaction(transactionMessage)
		if err != nil {
			slog.Debug("Error while sending transaction", "err", err)
			return err
		}
		err = c.protocol.WaitAck()
		if err != nil {
			slog.Debug("Error while waiting ack", "err", err)
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		slog.Debug("Error while scanning input file", "err", err)
		return err
	}

	err = c.protocol.SendEOF()
	if err != nil {
		slog.Debug("Error while sending EOF", "err", err)
		return err
	}

	return nil
}

func (c *Client) receiveResults() error {
	for !c.writer.DoneReceiving() {
		if !c.running.Load() {
			break
		}
		msg, err := c.protocol.ReceiveResult()
		if err != nil {
			slog.Debug("Error while receiving result", "err", err)
			return err
		}

		err = msg.Handle(c.writer)
		if err != nil {
			slog.Debug("Error while handling result", "err", err)
			return err
		}

	}
	return nil
}

func (c *Client) handleSignals() { // no estoy seguro de si esto va acá
	signals := make(chan os.Signal, 1)
	signal.Notify(
		signals,
		syscall.SIGINT,
		syscall.SIGTERM,
	)
	<-signals
	slog.Info("Received shutdown signal, stopping client...")
	c.running.Store(false)
	c.protocol.Close()
}
