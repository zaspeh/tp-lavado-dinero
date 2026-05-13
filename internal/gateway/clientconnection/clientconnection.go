package clientconnection

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/messagehandler"
)

type ClientConnection struct {
	id                  string
	protocol            *external.ExternalProtocol
	currencyFilterQueue m.Middleware
	// dateQueue      m.Middleware
	resultExchange     *m.ExchangeMiddleware
	transactionCounter int
}

func New(id string, protocol *external.ExternalProtocol, connSettings m.ConnSettings, currencyQueueName string, clientExchangeName string) (*ClientConnection, error) {
	currencyFilterQueue, err := m.CreateQueueMiddleware(currencyQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	exchangeRoutingKey := []string{clientExchangeName + "." + id}
	resultExchange, err := m.CreateExchangeMiddleware(clientExchangeName, exchangeRoutingKey, connSettings)
	if err != nil {
		currencyFilterQueue.Close()
		return nil, err
	}

	return &ClientConnection{
		id:                  id,
		protocol:            protocol,
		currencyFilterQueue: currencyFilterQueue,
		resultExchange:      resultExchange,
		transactionCounter:  0,
	}, nil
}

func (cc *ClientConnection) Run() error {
	go cc.resultExchange.StartConsuming(func(msg m.Message, ack, nack func()) {
		cc.handleResult(msg, ack, nack)
	})

	for {
		message, err := cc.protocol.ReceiveMsg()
		if err != nil {
			return err
		}
		if err = message.Handle(cc); err != nil {
			return err
		}
	}
}

func (cc *ClientConnection) HandleTransaction(msg message.Transaction) error {
	wrappedMessage, err := messagehandler.TransactionToProto(msg)
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	cc.transactionCounter++
	return nil
}

func (cc *ClientConnection) HandleEOF(msg message.EOF) error {
	wrappedMessage, err := messagehandler.EOFToProto(cc.id, cc.transactionCounter)
	if err != nil {
		return err
	}

	if err := cc.currencyFilterQueue.Send(*wrappedMessage); err != nil {
		return err
	}

	return nil
}

func (cc *ClientConnection) handleResult(msg m.Message, ack, nack func()) {

}

func (cc *ClientConnection) Close() error {
	err := cc.currencyFilterQueue.Close()
	if err != nil {
		return err
	}
	err = cc.resultExchange.Close()
	if err != nil {
		return err
	}
	return cc.protocol.Close()
}
