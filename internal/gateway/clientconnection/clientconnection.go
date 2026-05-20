package clientconnection

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/gateway/messagehandler"
)

const (
	eofAmountExpected = 5
)

type ClientConnection struct {
	id                  string
	protocol            *external.ExternalProtocol
	currencyFilterQueue m.Middleware
	// dateQueue      m.Middleware
	resultExchange     *m.ExchangeMiddleware
	EOFamountReceived  int
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

func (cc *ClientConnection) HandleTransaction(msg request.Transaction) error {
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

func (cc *ClientConnection) HandleEOF(msg request.EOF) error {
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
	moneyLaundry, err := serializer.DeserializeMoneyLaundering(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {
	case protobuf.MessageType_EOF:
		cc.handleEOFFromWorker(ack, nack)
	case protobuf.MessageType_MICROTRANSACTION_RESULT:
		cc.handleMicrotransactionResult(moneyLaundry, ack, nack)
	case protobuf.MessageType_MAXBANK_RESULT:
		cc.handleMaxBankResult(moneyLaundry, ack, nack)
	default:
		nack()
	}
}

func (cc *ClientConnection) handleEOFFromWorker(ack, nack func()) {
	cc.EOFamountReceived++
	if cc.EOFamountReceived == eofAmountExpected {
		if err := cc.protocol.SendEOF(); err != nil {
			cc.EOFamountReceived--
			nack()
			return
		}
	}
	ack()
}

func (cc *ClientConnection) handleMicrotransactionResult(moneyLaundry *protobuf.MoneyLaundry, ack, nack func()) {
	microtransactionResult, err := serializer.DeserializeTransaction(
		moneyLaundry.GetPayload(),
		&protobuf.MicrotransactionResult{},
	)

	if err != nil {
		nack()
		return
	}

	// TODO: usar message handler para convertir de proto a external
	msgResult := &result.MicrotransactionResult{
		Transactions: microtransactionResult.Transactions,
	}

	if err := cc.protocol.SendMicrotransactionResult(msgResult); err != nil {
		nack()
		return
	}

	ack()
}

func (cc *ClientConnection) handleMaxBankResult(moneyLaundering *protobuf.MoneyLaundry, ack, nack func()) {
	externalMsg, err := messagehandler.ProtoToMaxBankResult(moneyLaundering)
	if err != nil {
		nack()
		return
	}

	if err := cc.protocol.SendMaxBankResult(externalMsg); err != nil {
		nack()
		return
	}

	ack()
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
