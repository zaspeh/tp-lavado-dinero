package external

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
)

type ExternalProtocol struct {
	socket *socket.Socket
}

func New(socket *socket.Socket) *ExternalProtocol {
	return &ExternalProtocol{socket: socket}
}

func (p *ExternalProtocol) SendTransaction(transaction protocol.Transaction) error {
	return nil
}

func (p *ExternalProtocol) SendEOF() error {
	return nil
}

func (p *ExternalProtocol) SendResult() error {
	return nil
}

func (p *ExternalProtocol) Ack() error {
	return nil
}

func (p *ExternalProtocol) Nack() error {
	return nil
}

func (p *ExternalProtocol) ReceiveTransaction() (protocol.Transaction, error) {
	return protocol.Transaction{}, nil
}

func (p *ExternalProtocol) ReceiveResult() (protocol.Result, error) {
	return protocol.Result{}, nil
}

func (p *ExternalProtocol) ReceiveEOF() error {
	return nil
}

func (p *ExternalProtocol) WaitAck() error {
	return nil
}

func (p *ExternalProtocol) Close() error {
	return p.socket.Close()
}
