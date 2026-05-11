package serializer

import (
	"errors"

	"tp-lavado-dinero/common/external/protocol"
)

func SerializeTransaction(tx *protocol.Transaction) ([]byte, error) {
	data := SerializeString(tx.BankOrigin)

	data = append(data,
		SerializeString(tx.AccountOrigin)...,
	)

	data = append(data,
		SerializeString(tx.BankDestination)...,
	)

	data = append(data,
		SerializeString(tx.AccountDestination)...,
	)

	data = append(data,
		SerializeString(tx.Currency)...,
	)

	data = append(data,
		SerializeString(tx.PaymentType)...,
	)

	data = append(data,
		SerializeFloat64(tx.Amount)...,
	)

	data = append(data,
		SerializeInt64(tx.Timestamp)...,
	)

	return data, nil
}

func DeserializeTransaction(data []byte) (*protocol.Transaction, error) {
	offset := 0

	bankOriginBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	accountOriginBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	bankDestinationBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	accountDestinationBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	currencyBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	paymentTypeBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	if offset+FLOAT64_SIZE > len(data) {
		return nil, errors.New("invalid amount")
	}

	amount := DeserializeFloat64(
		data[offset : offset+FLOAT64_SIZE],
	)
	offset += FLOAT64_SIZE

	if offset+INT64_SIZE > len(data) {
		return nil, errors.New("invalid timestamp")
	}

	timestamp := DeserializeInt64(
		data[offset : offset+INT64_SIZE],
	)

	return &protocol.Transaction{
		BankOrigin:         DeserializeString(bankOriginBytes),
		AccountOrigin:      DeserializeString(accountOriginBytes),
		BankDestination:    DeserializeString(bankDestinationBytes),
		AccountDestination: DeserializeString(accountDestinationBytes),
		Currency:           DeserializeString(currencyBytes),
		PaymentType:        DeserializeString(paymentTypeBytes),
		Amount:             amount,
		Timestamp:          timestamp,
	}, nil
}
