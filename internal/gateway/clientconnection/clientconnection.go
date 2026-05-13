package clientconnection

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type ClientConnection struct {
	id       string
	protocol *external.ExternalProtocol
	usdQueue m.Middleware
	// dateQueue      m.Middleware
	resultExchange *m.ExchangeMiddleware
}

func New(id string, protocol *external.ExternalProtocol, connSettings m.ConnSettings, usdQueueName string, clientExchangeName string) (*ClientConnection, error) {
	usdQueue, err := m.CreateQueueMiddleware(usdQueueName, connSettings)
	if err != nil {
		return nil, err
	}

	exchangeRoutingKey := []string{clientExchangeName + "." + id}
	resultExchange, err := m.CreateExchangeMiddleware(clientExchangeName, exchangeRoutingKey, connSettings)
	if err != nil {
		usdQueue.Close()
		return nil, err
	}

	return &ClientConnection{
		id:             id,
		protocol:       protocol,
		usdQueue:       usdQueue,
		resultExchange: resultExchange,
	}, nil
}

func (cc *ClientConnection) Run() error {
	cc.resultExchange.StartConsuming(func(msg m.Message, ack, nack func()) {
		cc.handleResult(msg, ack, nack)
	})

	for {
		message, err := cc.protocol.ReceiveMsg()
		if err != nil {
			return err
		}
		message.Handle(cc)
	}
}

func (cc *ClientConnection) HandleTransaction(msg message.Transaction) error {
	return nil
}

func (cc *ClientConnection) HandleEOF(msg message.EOF) error {
	return nil
}

func (cc *ClientConnection) handleResult(msg m.Message, ack, nack func()) {

}

func (cc *ClientConnection) Close() {
	// TODO: CHECK ERR
	cc.usdQueue.Close()
	cc.resultExchange.Close()
}
