package messagehandler

import (
	"encoding/csv"
	"fmt"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	pb "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

func TransactionToProto(msg message.Transaction) (*pb.Transaction, error) {
	reader := csv.NewReader(strings.NewReader(msg.Record))

	fields, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error parsing csv: %w", err)
	}

	if len(fields) < 8 {
		return nil, fmt.Errorf("invalid record: expected 8 fields, got %d", len(fields))
	}

	fromBank, err := parseInt(fields[1])
	if err != nil {
		return nil, err
	}

	toBank, err := parseInt(fields[3])
	if err != nil {
		return nil, err
	}

	timestamp, err := time.Parse("2003-08-27 15:04:05", fields[0])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}

	transaction = &pb.Transaction{
		Timestamp:       timestamppb.New(timestamp),
		FromBank:        fromBank,
		ToBank:          toBank,
		Account:         fields[2],
		ToAccount:       fields[4],
		PaymentCurrency: fields[5],
		AmountPaid:      fields[6],
		PaymentFormat:   fields[7],
	}

	if marshalledTransaction, err := proto.Marshal(transaction); err != nil {
		return nil, fmt.Errorf("error marshalling transaction: %w", err)
	}

	wrappedMessage := m.Message{Body: marshalledTransaction}

	return wrappedMessage, nil
}
