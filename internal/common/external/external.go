package external

import (
	"errors"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
)

const (
	transaction uint8 = iota
	result
	eof
	ack
	nack
)

var (
	ErrMessageNotReceivedAck = errors.New("protocol error: did not received ack")
	ErrInvalidMessageType    = errors.New("protocol error: received invalid message type")
)

type ExternalProtocol struct {
	socket *socket.Socket
}

func New(socket *socket.Socket) *ExternalProtocol {
	return &ExternalProtocol{
		socket: socket,
	}
}

func (p *ExternalProtocol) sendMsgType(msgType uint8) error {
	serializeType := serializer.SerializeUint8(msgType)
	return p.socket.WriteAll(serializeType)
}

func (p *ExternalProtocol) receiveMsgType() (uint8, error) {
	msgTypeBytes, err := p.socket.ReadAll(serializer.ByteSize)
	if err != nil {
		return 0, err
	}
	return serializer.DeserializeUint8(msgTypeBytes), nil
}

func (p *ExternalProtocol) SendTransaction(transactionMessage message.Transaction) error {
	p.sendMsgType(transaction)
	serializeLength := serializer.SerializeUint16(uint16(len(transactionMessage.Record)))
	serializeString := serializer.SerializeString(transactionMessage.Record)
	err := p.socket.WriteAll(append(serializeLength, serializeString...))
	if err != nil {
		return err
	}
	return nil
}

func (p *ExternalProtocol) SendResult() error {
	return nil
}

func (p *ExternalProtocol) SendEOF() error {
	return p.sendMsgType(eof)
}

func (p *ExternalProtocol) SendAck() error {
	return p.sendMsgType(ack)
}

func (p *ExternalProtocol) SendNack() error {
	return p.sendMsgType(nack)
}

// Only use on gateway, might need to split protocol.
func (p *ExternalProtocol) ReceiveMsg() (message.Message, error) {
	msgType, err := p.receiveMsgType()
	if err != nil {
		return nil, err
	}
	switch msgType {
	case transaction:
		return p.ReceiveTransaction()
	case eof:
		return message.EOF{}, nil
	default:
		return nil, ErrInvalidMessageType
	}
}

func (p *ExternalProtocol) ReceiveTransaction() (message.Transaction, error) {
	stringLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
	if err != nil {
		return message.Transaction{}, err
	}
	stringLength := serializer.DeserializeUint16(stringLengthBytes)
	stringBytes, err := p.socket.ReadAll(int(stringLength))
	if err != nil {
		return message.Transaction{}, err
	}
	record := serializer.DeserializeString(stringBytes)
	return message.NewTransaction(record), nil
}

func (p *ExternalProtocol) ReceiveResult() error {
	return nil
}

func (p *ExternalProtocol) WaitAck() error {
	ackByte, err := p.socket.ReadAll(serializer.ByteSize)
	deserializeAck := serializer.DeserializeUint8(ackByte)
	if err != nil {
		return err
	}
	if deserializeAck != ack {
		return ErrMessageNotReceivedAck
	}
	return nil
}

func (p *ExternalProtocol) Close() error {
	return p.socket.Close()
}
