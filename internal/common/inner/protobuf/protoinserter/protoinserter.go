package protoinserter

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

func InsertConvertedAmountBatch(clientID string, batch *protobuf.ConvertedAmountBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ConvertedAmountBatch{
		ConvertedAmountBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		innerMessage,
	)
}

func InsertMicrotransactionBatch(clientID string, batch *protobuf.MicrotransactionBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MicrotransactionsBatch{
		MicrotransactionsBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MICROTRANSACTION_BATCH,
		innerMessage,
	)
}

func InsertMaxBankBatch(clientID string, batch *protobuf.MaxBankBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MaxBankBatch{
		MaxBankBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MAXBANK_BATCH,
		innerMessage,
	)
}

func InsertMaxBankResultBatch(clientID string, batch *protobuf.MaxBankResultBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MaxBankResultBatch{
		MaxBankResultBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MAX_BANK_RESULT_BATCH,
		innerMessage,
	)
}

func InsertAvgByTypeTransactionBatch(clientID string, batch *protobuf.AvgByTypeTransactionBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_AvgbytypeTransactionBatch{
		AvgbytypeTransactionBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH,
		innerMessage,
	)
}
