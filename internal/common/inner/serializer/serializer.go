package serializer

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

func SerializeMoneyLaundering(moneyLaundering *protobuf.MoneyLaundry) (*middleware.Message, error) {
	marshalledMoneyLaundering, err := proto.Marshal(moneyLaundering)
	if err != nil {
		return nil, fmt.Errorf("error marshalling money laundry: %w", err)
	}

	return &middleware.Message{
		Body: marshalledMoneyLaundering,
	}, nil
}

func DeserializeMoneyLaundering(message middleware.Message) (*protobuf.MoneyLaundry, error) {
	moneyLaundering := &protobuf.MoneyLaundry{}
	if err := proto.Unmarshal(message.Body, moneyLaundering); err != nil {
		return nil, fmt.Errorf("error unmarshalling money laundry: %w", err)
	}
	return moneyLaundering, nil
}

func SerializeTransaction(transaction *protobuf.Transaction) (*middleware.Message, error) {
	marshalledTransaction, err := proto.Marshal(transaction)
	if err != nil {
		return nil, fmt.Errorf("error marshalling transaction: %w", err)
	}

	return &middleware.Message{
		Body: marshalledTransaction,
	}, nil
}

func DeserializeTransaction(message middleware.Message) (*protobuf.Transaction, error) {
	transaction := &protobuf.Transaction{}
	if err := proto.Unmarshal(message.Body, transaction); err != nil {
		return nil, fmt.Errorf("error unmarshalling transaction: %w", err)
	}
	return transaction, nil
}
