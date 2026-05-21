package messagehandler

import (
	"encoding/csv"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	pb "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

func TransactionToProto(clientID string, msg request.Transaction) (*m.Message, error) {
	reader := csv.NewReader(strings.NewReader(msg.Record))

	fields, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error parsing csv: %w", err)
	}

	if len(fields) < 8 {
		return nil, fmt.Errorf("invalid record: expected 8 fields, got %d", len(fields))
	}

	fromBank, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}

	toBank, err := strconv.Atoi(fields[3])
	if err != nil {
		return nil, err
	}

	timestamp, err := time.Parse("2006/01/02 15:04", fields[0])
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp: %w", err)
	}
	/* debug csv parsing
	slog.Info(
		"parsed csv",
		"f0", fields[0],
		"f1", fields[1],
		"f2", fields[2],
		"f3", fields[3],
		"f4", fields[4],
		"f5", fields[5],
		"f6", fields[6],
		"f7", fields[7],
	)
	*/

	transaction := &pb.Transaction{
		ClientID:        clientID,
		Timestamp:       timestamppb.New(timestamp),
		FromBank:        int32(fromBank),
		ToBank:          int32(toBank),
		Account:         fields[2],
		ToAccount:       fields[4],
		PaymentCurrency: fields[6], // ojo con esto
		AmountPaid:      fields[5], // ojo con esto
		PaymentFormat:   fields[7],
	}

	return serializer.SerializeProtoMessage(transaction, pb.MessageType_TRANSACTION)

}

func EOFToProto(clientID string, transactionCounter int) (*m.Message, error) {
	eofMessage := &pb.EOF{
		ClientID:          clientID,
		TotalTransactions: int32(transactionCounter),
	}
	return serializer.SerializeProtoMessageWithClientID(clientID, eofMessage, pb.MessageType_EOF_)
}

func ProtoToMaxBankResult(moneyLaundering *protobuf.MoneyLaundry) ([]result.MaxBankResult, error) {
	batch, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.MaxBankResultBatch{})
	if err != nil {
		return nil, err
	}
	slog.Info("ProtoToMaxBankResult", "results_count", len(batch.GetResults()))

	results := batch.GetResults()
	externalMessage := make([]result.MaxBankResult, 0, len(results))
	for _, r := range batch.GetResults() {
		externalMessage = append(externalMessage, result.MaxBankResult{
			BankName: r.GetBankName(),
			Account:  r.GetAccount(),
			Amount:   r.GetAmount(),
		})
	}
	return externalMessage, nil
}
