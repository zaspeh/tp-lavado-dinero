package client

import (
	"bufio"
	"fmt"
	"log/slog"
	"math"
	"net"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/client/storage"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
)

type ClientConfig struct {
	ServerHost     string
	ServerPort     string
	InputFile      string
	OutputDir      string
	MaxBatchWeight int
}

type Client struct {
	config           ClientConfig
	running          atomic.Bool
	protocol         *external.ExternalProtocol
	writer           *storage.ResultCSVWriter
	transactionBatch *batch.Batch[request.Transaction, []request.Transaction]
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
	err = writer.Open()
	if err != nil {
		socket.Close()
		return nil, err
	}

	externalProtocol := external.New(socket)
	sizer := externalProtocol.TransactionSize
	wrapper := request.NewTransactionBatch

	transactionBatch := batch.New(
		config.MaxBatchWeight-externalProtocol.HeaderSizeForTransactions(),
		sizer,
		wrapper,
	)

	client := &Client{
		config:           config,
		protocol:         external.New(socket),
		writer:           writer,
		transactionBatch: transactionBatch,
	}

	client.running.Store(true)
	return client, nil
}

func (c *Client) Run() error {
	defer c.protocol.Close()
	defer c.writer.Close()

	go c.handleSignals()

	slog.Info("starting processTransactions")

	err := c.processTransactions()
	if err != nil {
		return err
	}

	slog.Info("starting receiveResults")

	err = c.receiveResults()
	if err != nil {
		return err
	}

	slog.Info("finished receiveResults")

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
	scanner.Scan() // skip header

	// Closure necesario por el uso de scanner, revisar como mejorarlo
	next := func() (request.Transaction, bool) {
		if !scanner.Scan() {
			return request.Transaction{}, false
		}
		return request.NewTransaction(scanner.Text()), true
	}

	err = batch.ForEachFlushed(c.transactionBatch, next, c.sendTransactionBatch)
	if err != nil {
		slog.Debug("Error while processing transactions", "err", err)
		return err
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

	return c.protocol.WaitAck()
}

func (c *Client) sendTransactionBatch(transactions []request.Transaction) error {
	if err := c.protocol.SendTransactionBatch(transactions); err != nil {
		return err
	}
	return c.protocol.WaitAck()
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

		slog.Info(
			"result received",
			"type",
			fmt.Sprintf("%T", msg),
		)

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
