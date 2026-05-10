package client

import (
	"fmt"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"tp-lavado-dinero/client/communication/receiver"
	"tp-lavado-dinero/client/communication/sender"
	"tp-lavado-dinero/client/reader"
	"tp-lavado-dinero/common/external/protocol"
)

type ClientConfig struct {
	ServerHost string
	ServerPort string

	InputFile string
	OutputDir string

	ClientID string
	JobID    string

	ChunkSize int
}

type Client struct {
	config  ClientConfig
	conn    net.Conn
	running atomic.Bool
}

func New(config ClientConfig) (*Client, error) {
	address := fmt.Sprintf(
		"%s:%s",
		config.ServerHost,
		config.ServerPort,
	)

	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	client := &Client{
		config: config,
		conn:   conn,
	}

	client.running.Store(true)

	return client, nil
}

func (c *Client) Run() error {
	defer c.conn.Close()

	go c.handleSignals()

	err := reader.ReadTransactions( // leo las transacciones desde el reader
		c.config.InputFile,
		func(tx *protocol.Transaction) error { // por cada tx que leo, la envío en el sender
			return sender.SendTransaction(
				c.conn,
				c.config.JobID,
				c.config.ClientID,
				tx,
			)
		},
	)

	if err != nil {
		if c.running.Load() {
			return err
		}
		return nil
	}

	if err := sender.SendEOF( // una vez enviadas todas las tx envio el EOF desde el sender
		c.conn,
		c.config.JobID,
		c.config.ClientID,
	); err != nil {
		if c.running.Load() {
			return err
		}
		return nil
	}

	if err := receiver.ReceiveResults( // recibo los resultados de lo que envíe -> busywait?
		c.conn,
		c.config.OutputDir,
	); err != nil {
		if c.running.Load() {
			return err
		}
		return nil
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

	c.running.Store(false)

	c.conn.Close()
}
