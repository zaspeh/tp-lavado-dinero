package messagehandler

import (
	"encoding/csv"
	"fmt"
	"strconv"
	"strings"
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/google/uuid"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/request"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/message/result"
	m "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/serializer"
)

func RawTransactionToProtoTransaction(msg request.Transaction) (*protobuf.Transaction, error) {
	reader := csv.NewReader(strings.NewReader(msg.Record))
	fields, err := reader.Read()

	if err != nil {
		return nil, fmt.Errorf("error parsing csv: %w", err)
	}

	if len(fields) < 10 {
		return nil, fmt.Errorf("invalid record: expected 10 fields, got %d", len(fields))
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

	transaction := &protobuf.Transaction{
		Timestamp:       timestamppb.New(timestamp),
		FromBank:        int32(fromBank),
		ToBank:          int32(toBank),
		Account:         fields[2],
		ToAccount:       fields[4],
		AmountPaid:      fields[7],
		PaymentCurrency: fields[8],
		PaymentFormat:   fields[9],
	}
	return transaction, nil
}

func EOFToProto(clientID string, transactionCounter int) (m.Message, error) {
	eofID := uuid.New().String()
	eofMessage := &protobuf.MoneyLaundry_EofMessage{
		EofMessage: &protobuf.EOF{
			TotalTransactions: uint64(transactionCounter),
		},
	}
	return protobuf.SerializeProtoMessageONTRIAL(clientID, protobuf.MessageType_EOF_, eofMessage, eofID)
}

func ProtoTransactionToProtoConvTransaction(msg *protobuf.Transaction) *protobuf.ToConvertTransaction {
	convertionTransaction := &protobuf.ToConvertTransaction{
		Timestamp:       msg.GetTimestamp(),
		AmountPaid:      msg.GetAmountPaid(),
		PaymentCurrency: msg.GetPaymentCurrency(),
		PaymentFormat:   msg.GetPaymentFormat(),
	}
	return convertionTransaction
}

func ProtoToMaxBankResult(moneyLaundering *protobuf.MoneyLaundry) ([]result.MaxBankResult, error) {
	batch := moneyLaundering.GetMaxBankResultBatch()
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

func ProtoToAvgByTypeResults(moneyLaundering *protobuf.MoneyLaundry) ([]result.AvgByTypeResult, error) {
	batch, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.AvgByTypeResultBatch{})
	if err != nil {
		return nil, err
	}

	results := batch.GetResults()
	externalMessage := make([]result.AvgByTypeResult, 0, len(results))
	for _, r := range results {
		externalMessage = append(externalMessage, result.AvgByTypeResult{
			Account:    r.GetAccount(),
			AmountPaid: r.GetAmountPaid(),
		})
	}
	return externalMessage, nil
}

func ProtoToConvertedMicroPaymentResult(moneyLaundering *protobuf.MoneyLaundry) (*result.ConvertedMicroPaymentResult, error) {
	deserializeMsg, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.ConvertedMicroPaymentResult{})
	if err != nil {
		return nil, err
	}

	return &result.ConvertedMicroPaymentResult{
		Count: deserializeMsg.GetCount(),
	}, nil
}

func ProtoToSuspiciousAccounts(moneyLaundering *protobuf.MoneyLaundry,
) (*result.SuspiciousAccountsResult, error) {

	batch, err := serializer.DeserializeTransaction(moneyLaundering.GetPayload(), &protobuf.SuspiciousAccountBatch{})
	if err != nil {
		return nil, err
	}

	accounts := make([]result.SuspiciousAccount, 0, len(batch.GetAccounts()))

	for _, account := range batch.GetAccounts() {

		accounts = append(accounts, result.SuspiciousAccount{
			Bank:    account.GetBank(),
			Account: account.GetAccount(),
		})
	}

	return &result.SuspiciousAccountsResult{
		Accounts: accounts,
	}, nil

}

func RawAccountToProtoMaxBank(account request.Account) (*protobuf.MaxBank, error) {
	reader := csv.NewReader(strings.NewReader(account.Record))
	fields, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error parsing csv: %w", err)
	}

	if len(fields) < 5 {
		return nil, fmt.Errorf("invalid record: expected 5 fields, got %d", len(fields))
	}

	bankID, err := strconv.Atoi(fields[1])
	if err != nil {
		return nil, err
	}

	bankMetadata := &protobuf.BankMetadata{
		BankName: fields[0],
	}

	return &protobuf.MaxBank{
		FromBank: int32(bankID),
		Payload: &protobuf.MaxBank_BankMetadata{
			BankMetadata: bankMetadata,
		},
	}, nil
}
