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
	ServerHost           string
	ServerPort           string
	TransactionInputFile string
	AccountsInputFile    string
	OutputDir            string
	MaxBatchWeight       int
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
	err = writer.Open()
	if err != nil {
		socket.Close()
		return nil, err
	}

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
	defer c.writer.Close()

	go c.handleSignals()

	startTimestamp := time.Now()

	slog.Info("starting processTransactions")

	err := c.processTransactions()
	if err != nil {
		return err
	}

	slog.Info("starting processAccounts")
	err = c.processAccounts()
	if err != nil {
		return err
	}

	slog.Info("starting receiveResults")

	err = c.receiveResults()
	if err != nil {
		return err
	}

	slog.Info("finished processing results", "duration", time.Since(startTimestamp).String())

	slog.Info("finished receiveResults")

	return nil
}

func buildBatcher[T any, B any](
	c *Client,
	headerSize int,
	sizer func(T) int,
	wrapper func([]T) B,
	onFlush func(B) error,
) *batch.Batcher[T, B] {

	availableWeight := c.config.MaxBatchWeight - headerSize
	innerBatch := batch.New(availableWeight, sizer, wrapper)
	return batch.NewBatcher(innerBatch, onFlush)
}

func (c *Client) buildTransactionBatcher() *batch.Batcher[request.Transaction, request.TransactionBatch] {
	return buildBatcher(
		c,
		c.protocol.HeaderSizeForTransactions(),
		c.protocol.TransactionSize,
		request.NewTransactionBatch,
		c.sendTransactionBatch,
	)
}

func (c *Client) buildAccountBatcher() *batch.Batcher[request.Account, request.AccountBatch] {
	return buildBatcher(
		c,
		c.protocol.HeaderSizeForAccounts(),
		c.protocol.AccountSize,
		request.NewAccountBatch,
		c.sendAccountBatch,
	)
}

func (c *Client) processTransactions() error {
	transactionsFile, err := os.Open(c.config.TransactionInputFile)
	if err != nil {
		slog.Debug("Error while runninging input file", "err", err)
		return err
	}
	defer transactionsFile.Close()

	TransactionScanner := bufio.NewScanner(transactionsFile)
	TransactionScanner.Scan() // skip header

	batcher := c.buildTransactionBatcher()

	for TransactionScanner.Scan() {
		record := TransactionScanner.Text()
		transaction := request.NewTransaction(record)
		if err := batcher.Add(transaction); err != nil {
			slog.Debug("Error while adding transaction to batcher", "err", err)
			return err
		}
	}

	// liberamos el batcher por si quedó algo pendiente
	if err := batcher.Flush(); err != nil {
		slog.Debug("Error while flushing batcher", "err", err)
		return err
	}

	return TransactionScanner.Err()
}

func (c *Client) processAccounts() error {
	accountsFile, err := os.Open(c.config.AccountsInputFile)
	if err != nil {
		slog.Debug("Error while runninging accounts file", "err", err)
		return err
	}
	defer accountsFile.Close()

	accountsScanner := bufio.NewScanner(accountsFile)
	accountsScanner.Scan() // skip header

	batcher := c.buildAccountBatcher()

	for accountsScanner.Scan() {
		record := accountsScanner.Text()
		account := request.NewAccount(record)
		if err := batcher.Add(account); err != nil {
			slog.Debug("Error while adding account to batcher", "err", err)
			return err
		}
	}

	if err := batcher.Flush(); err != nil {
		slog.Debug("Error while flushing batcher", "err", err)
		return err
	}

	if err := accountsScanner.Err(); err != nil {
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

func (c *Client) sendTransactionBatch(transactions request.TransactionBatch) error {
	if err := c.protocol.SendTransactionBatch(transactions); err != nil {
		return err
	}
	return c.protocol.WaitAck()
}

func (c *Client) sendAccountBatch(accounts request.AccountBatch) error {
	if err := c.protocol.SendAccountBatch(accounts); err != nil {
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

		slog.Debug(
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
