package protoinserters

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

func InsertConvertedAmountBatch(clientID string, batchID string, batch *protobuf.ConvertedAmountBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ConvertedAmountBatch{
		ConvertedAmountBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertMicrotransactionBatch(clientID string, batchID string, batch *protobuf.MicrotransactionBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MicrotransactionsBatch{
		MicrotransactionsBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MICROTRANSACTION_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertPeriodFilterBatch(clientID string, batchID string, batch *protobuf.PeriodFilterBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_PeriodFilterBatch{
		PeriodFilterBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_PERIOD_FILTER_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertMaxBankBatch(clientID string, batchID string, batch *protobuf.MaxBankBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MaxBankBatch{
		MaxBankBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MAXBANK_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertMaxBankResultBatch(clientID string, batchID string, batch *protobuf.MaxBankResultBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_MaxBankResultBatch{
		MaxBankResultBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_MAX_BANK_RESULT_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertAvgByTypeTransactionBatch(clientID string, batchID string, batch *protobuf.AvgByTypeTransactionBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_AvgbytypeTransactionBatch{
		AvgbytypeTransactionBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_AVGBYTYPE_TRANSACTION_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertToConvertPeriodFilteredBatch(clientID string, batchID string, batch *protobuf.ToConvertPeriodFilteredBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ToConvertPeriodFilteredBatch{
		ToConvertPeriodFilteredBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_TO_CONVERT_PERIOD_FILTERED_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertToConvertTypeFilteredPaymentBatch(clientID string, batchID string, batch *protobuf.ToConvertTypeFilteredPaymentBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ToConvertTypeFilteredPaymentBatch{
		ToConvertTypeFilteredPaymentBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_TO_CONVERT_TYPE_FILTERED_PAYMENT_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertScatterGatherBatch(clientID string, batchID string, batch *protobuf.ScatterGatherBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ScattergatherBatch{
		ScattergatherBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_SCATTERGATHER_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertGroupedAccountsBatch(clientID string, batchID string, batch *protobuf.GroupedAccountsBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_GroupedAccountsBatch{
		GroupedAccountsBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_GROUPED_ACCOUNTS_BATCH,
		innerMessage,
		batchID,
	)
}

func InsertIntermediaryPairBatch(clientID string, batchID string, batch *protobuf.IntermediaryPairBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_IntermediarypairBatch{
		IntermediarypairBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_INTERMEDIARYPAIR_BATCH,
		innerMessage,
		batchID,
	)
}
