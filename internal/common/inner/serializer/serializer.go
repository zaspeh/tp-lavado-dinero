package serializer

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

func serializeMoneyLaundering(moneyLaundering *protobuf.MoneyLaundry) (*middleware.Message, error) {
	marshalledMoneyLaundering, err := proto.Marshal(moneyLaundering)
	if err != nil {
		return nil, fmt.Errorf("error marshalling money laundry: %w", err)
	}

	return &middleware.Message{
		Body: marshalledMoneyLaundering,
	}, nil
}

func SerializeProtoMessageWithClientID[T proto.Message](
	clientID string,
	transaction T,
	messageType protobuf.MessageType,
) (*middleware.Message, error) {

	marshalledTransaction, err := proto.Marshal(transaction)
	if err != nil {
		return nil, fmt.Errorf("error marshalling transaction: %w", err)
	}

	moneyLaundering := &protobuf.MoneyLaundry{
		ClientID: clientID,
		Type:     messageType,
		Payload:  marshalledTransaction,
	}

	return serializeMoneyLaundering(moneyLaundering)
}

func DeserializeMoneyLaundering(message middleware.Message) (*protobuf.MoneyLaundry, error) {
	moneyLaundering := &protobuf.MoneyLaundry{}
	if err := proto.Unmarshal(message.Body, moneyLaundering); err != nil {
		return nil, fmt.Errorf("error unmarshalling money laundry: %w", err)
	}
	return moneyLaundering, nil
}

func SerializeProtoMessage[T proto.Message](transaction T, messageType protobuf.MessageType) (*middleware.Message, error) {
	marshalledTransaction, err := proto.Marshal(transaction)
	if err != nil {
		return nil, fmt.Errorf("error marshalling transaction: %w", err)
	}

	moneyLaundering := &protobuf.MoneyLaundry{
		Type:    messageType,
		Payload: marshalledTransaction,
	}

	return serializeMoneyLaundering(moneyLaundering)
}

func DeserializeTransaction[T proto.Message](payload []byte, transaction T) (T, error) {
	if err := proto.Unmarshal(payload, transaction); err != nil {
		var zero T

		return zero, fmt.Errorf("error unmarshalling transaction: %w", err)
	}
	return transaction, nil
}
