package external

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	transaction uint8 = iota
	transactionBatch
	accountBatch
	microtransactionResult
	maxBankResult
	convertedMicroPaymentResult
	avgByTypeResult
	suspiciousAccountsResult

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
	mu     sync.Mutex
}

func New(socket *socket.Socket) *ExternalProtocol {
	return &ExternalProtocol{
		socket: socket,
	}
}

func (p *ExternalProtocol) HeaderSizeForTransactions() int {
	return serializer.ByteSize + serializer.Uint16Size // msgType + BatchCount
}

func (p *ExternalProtocol) TransactionSize(transaction request.Transaction) int {
	return serializer.Uint16Size + len(transaction.Record) // length + record bytes
}

func (p *ExternalProtocol) HeaderSizeForAccounts() int {
	return p.HeaderSizeForTransactions()
}

func (p *ExternalProtocol) AccountSize(account request.Account) int {
	return serializer.Uint16Size + len(account.Record)
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

func (p *ExternalProtocol) SendTransactionBatch(transactions []request.Transaction) error {
	if len(transactions) == 0 {
		return nil
	}

	totalSize := p.HeaderSizeForTransactions()

	payloads := make([][]byte, len(transactions))
	for i, tx := range transactions {
		payloads[i] = serializer.SerializeString(tx.Record)
		totalSize += p.TransactionSize(tx)
	}

	buf := make([]byte, 0, totalSize)
	buf = append(buf, serializer.SerializeUint8(transactionBatch)...)
	buf = append(buf, serializer.SerializeUint16(uint16(len(transactions)))...)
	for i, tx := range transactions {
		buf = append(buf, serializer.SerializeUint16(uint16(len(tx.Record)))...)
		buf = append(buf, payloads[i]...)
	}

	return p.socket.WriteAll(buf)
}

// TODO: refactor para evitar logica repetida
func (p *ExternalProtocol) SendAccountBatch(accounts []request.Account) error {
	if len(accounts) == 0 {
		return nil
	}

	totalSize := p.HeaderSizeForAccounts()

	payloads := make([][]byte, len(accounts))
	for i, account := range accounts {
		payloads[i] = serializer.SerializeString(account.Record)
		totalSize += p.AccountSize(account)
	}

	buf := make([]byte, 0, totalSize)
	buf = append(buf, serializer.SerializeUint8(accountBatch)...)
	buf = append(buf, serializer.SerializeUint16(uint16(len(accounts)))...)
	for i, account := range accounts {
		buf = append(buf, serializer.SerializeUint16(uint16(len(account.Record)))...)
		buf = append(buf, payloads[i]...)
	}

	return p.socket.WriteAll(buf)
}

func (p *ExternalProtocol) SendMicrotransactionResult(result *result.MicrotransactionResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()
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
	p.mu.Lock()
	defer p.mu.Unlock()
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

func (p *ExternalProtocol) SendConvertedMicroPaymentResult(result *result.ConvertedMicroPaymentResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if err := p.sendMsgType(convertedMicroPaymentResult); err != nil {
		return err
	}

	count := result.Count
	//TODO: revisar si se puede usar uint en proto y evitar esta conversión
	serializeCount := serializer.SerializeUint32(uint32(count))
	return p.socket.WriteAll(serializeCount)

}

func (p *ExternalProtocol) SendAvgByTypeResult(result *result.AvgByTypeResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if err := p.sendMsgType(avgByTypeResult); err != nil {
		return err
	}

	record := fmt.Sprintf("%s,%s", result.Account, result.AmountPaid)

	length := serializer.SerializeUint16(
		uint16(len(record)),
	)

	data := serializer.SerializeString(record)

	return p.socket.WriteAll(append(length, data...))
}

func (p *ExternalProtocol) SendSuspiciousAccountsResult(result *result.SuspiciousAccountsResult) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	for _, account := range result.Accounts {

		if err := p.sendMsgType(
			suspiciousAccountsResult,
		); err != nil {
			return err
		}

		record := fmt.Sprintf("%d,%s", account.Bank, account.Account)

		length := serializer.SerializeUint16(uint16(len(record)))

		data := serializer.SerializeString(record)

		if err := p.socket.WriteAll(append(length, data...)); err != nil {
			return err
		}
	}

	return nil
}

func (p *ExternalProtocol) SendEOF() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.sendMsgType(eof)
}

func (p *ExternalProtocol) SendAck() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.sendMsgType(ack)
}

func (p *ExternalProtocol) SendNack() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.sendMsgType(nack)
}

// Only use on gateway, might need to split protocol.
func (p *ExternalProtocol) ReceiveMsg() (request.Message, error) {
	msgType, err := p.receiveMsgType()
	if err != nil {
		return nil, err
	}
	switch msgType {
	case transactionBatch:
		return p.receiveTransactionBatch()
	case accountBatch:
		return p.receiveAccountBatch()
	case eof:
		return request.EOF{}, nil
	default:
		return nil, ErrInvalidMessageType
	}
}

func (p *ExternalProtocol) receiveTransactionBatch() (request.TransactionBatch, error) {
	batchLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
	if err != nil {
		return nil, err
	}
	batchLength := serializer.DeserializeUint16(batchLengthBytes)
	transactions := make([]request.Transaction, batchLength)
	for i := range transactions {
		stringLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
		if err != nil {
			return nil, err
		}
		stringLength := serializer.DeserializeUint16(stringLengthBytes)
		stringBytes, err := p.socket.ReadAll(int(stringLength))
		if err != nil {
			return nil, err
		}
		record := serializer.DeserializeString(stringBytes)
		transactions[i] = request.NewTransaction(record)
	}
	return request.NewTransactionBatch(transactions), nil
}

// TODO: refactorizar logica repetida
func (p *ExternalProtocol) receiveAccountBatch() (request.AccountBatch, error) {
	batchLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
	if err != nil {
		return nil, err
	}
	batchLength := serializer.DeserializeUint16(batchLengthBytes)
	accounts := make([]request.Account, batchLength)
	for i := range accounts {
		stringLengthBytes, err := p.socket.ReadAll(serializer.Uint16Size)
		if err != nil {
			return nil, err
		}
		stringLength := serializer.DeserializeUint16(stringLengthBytes)
		stringBytes, err := p.socket.ReadAll(int(stringLength))
		if err != nil {
			return nil, err
		}
		record := serializer.DeserializeString(stringBytes)
		accounts[i] = request.NewAccount(record)
	}
	return request.NewAccountBatch(accounts), nil
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
	case avgByTypeResult:
		return p.receiveAvgByTypeResult()
	case eof:
		return result.EOF{}, nil
	case convertedMicroPaymentResult:
		return p.receiveConvertedMicroPaymentResult()
	case suspiciousAccountsResult:
		return p.receiveSuspiciousAccountResult()
	default:
		return nil, fmt.Errorf(
			"protocol error: invalid message type %d",
			msgType,
		)
	}
}

func (p *ExternalProtocol) receiveAvgByTypeResult() (result.Result, error) {
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

	if len(fields) != 2 {
		return nil, fmt.Errorf(
			"invalid avg by type result record",
		)
	}

	return result.AvgByTypeResult{
		Account:    fields[0],
		AmountPaid: fields[1],
	}, nil
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

func (p *ExternalProtocol) receiveConvertedMicroPaymentResult() (result.Result, error) {
	countBytes, err := p.socket.ReadAll(serializer.Uint32Size)
	if err != nil {
		return nil, err
	}

	count := serializer.DeserializeUint32(countBytes)

	// TODO: revisar si se puede usar uint en proto y evitar esta conversión
	return &result.ConvertedMicroPaymentResult{
		Count: int64(count),
	}, nil
}

func (p *ExternalProtocol) receiveSuspiciousAccountResult() (result.Result, error) {

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

	if len(fields) != 2 {
		return nil, fmt.Errorf(
			"invalid suspicious account result",
		)
	}

	bank, err := strconv.Atoi(fields[0])

	if err != nil {
		return nil, err
	}

	return result.SuspiciousAccountsResult{
		Accounts: []result.SuspiciousAccount{
			{
				Bank:    int32(bank),
				Account: fields[1],
			},
		},
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
