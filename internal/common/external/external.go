package external

import (
	"errors"
	"fmt"
	"strings"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	transaction uint8 = iota

	microtransactionResult
	maxBankResult

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

func (p *ExternalProtocol) SendTransaction(transactionMessage request.Transaction) error {
	if err := p.sendMsgType(transaction); err != nil {
		return err
	}
	serializeLength := serializer.SerializeUint16(uint16(len(transactionMessage.Record)))
	serializeString := serializer.SerializeString(transactionMessage.Record)
	err := p.socket.WriteAll(append(serializeLength, serializeString...))
	if err != nil {
		return err
	}
	return nil
}

func (p *ExternalProtocol) SendResult() error { // podríamos borrarlo
	return nil
}

func (p *ExternalProtocol) SendMicrotransactionResult(result *result.MicrotransactionResult) error {
	if err := p.sendMsgType(microtransactionResult); err != nil {
		return err
	}

	protoResult := &protobuf.MicrotransactionResult{
		Transactions: result.Transactions,
	}

	data, err := proto.Marshal(protoResult)
	if err != nil {
		return err
	}

	length := serializer.SerializeUint32(
		uint32(len(data)),
	)

	if err := p.socket.WriteAll(length); err != nil {
		return err
	}

	return p.socket.WriteAll(data)
}

func (p *ExternalProtocol) SendMaxBankResult(results []result.MaxBankResult) error {
	for _, res := range results {
		if err := p.sendMsgType(maxBankResult); err != nil {
			return err
		}
		record := fmt.Sprintf("%s,%s,%s", res.BankName, res.Account, res.Amount)
		serializeLength := serializer.SerializeUint16(uint16(len(record)))
		serializeString := serializer.SerializeString(record)

		if err := p.socket.WriteAll(append(serializeLength, serializeString...)); err != nil {
			return err
		}
	}
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
func (p *ExternalProtocol) ReceiveMsg() (request.Message, error) {
	msgType, err := p.receiveMsgType()
	if err != nil {
		return nil, err
	}
	switch msgType {
	case transaction:
		return p.ReceiveTransaction()
	case eof:
		return request.EOF{}, nil
	default:
		return nil, ErrInvalidMessageType
	}
}

func (p *ExternalProtocol) ReceiveTransaction() (request.Transaction, error) {
	stringLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
	if err != nil {
		return request.Transaction{}, err
	}
	stringLength := serializer.DeserializeUint16(stringLengthBytes)
	stringBytes, err := p.socket.ReadAll(int(stringLength))
	if err != nil {
		return request.Transaction{}, err
	}
	record := serializer.DeserializeString(stringBytes)
	return request.NewTransaction(record), nil
}

func (p *ExternalProtocol) ReceiveResult() (result.Result, error) {
	msgType, err := p.receiveMsgType()
	if err != nil {
		return nil, err
	}

	switch msgType {

	case microtransactionResult:
		return p.receiveMicrotransactionResult()

	case maxBankResult:
		return p.receiveMaxBankResult()

	case eof:
		return result.EOF{}, nil

	default:
		return nil, ErrInvalidMessageType
	}
}

func (p *ExternalProtocol) receiveMicrotransactionResult() (result.Result, error) {
	lengthBytes, err := p.socket.ReadAll(serializer.Uint32Size)
	if err != nil {
		return nil, err
	}

	length := serializer.DeserializeUint32(lengthBytes)

	data, err := p.socket.ReadAll(int(length))
	if err != nil {
		return nil, err
	}

	protoResult := &protobuf.MicrotransactionResult{}

	if err := proto.Unmarshal(data, protoResult); err != nil {
		return nil, err
	}

	return &result.MicrotransactionResult{
		Transactions: protoResult.Transactions,
	}, nil
}

func (p *ExternalProtocol) receiveMaxBankResult() (result.Result, error) {
	stringLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
	if err != nil {
		return nil, err
	}

	length := serializer.DeserializeUint16(stringLengthBytes)
	stringBytes, err := p.socket.ReadAll(int(length))
	if err != nil {
		return nil, err
	}

	record := serializer.DeserializeString(stringBytes)
	fields := strings.Split(record, ",")
	if len(fields) != 3 {
		return nil, fmt.Errorf("invalid max bank result record: expected 3 fields, got %d", len(fields))
	}

	return result.MaxBankResult{
		BankName: fields[0],
		Account:  fields[1],
		Amount:   fields[2],
	}, nil
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
