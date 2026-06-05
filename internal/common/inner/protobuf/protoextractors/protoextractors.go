package protoextractors

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"

func GetConvertedAmountBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.ConvertedAmount {
	return batch.GetConvertedAmountBatch().GetItems()
}

func GetMaxBankBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.MaxBank {
	return batch.GetMaxBankBatch().GetMaxBankMessage()
}

func GetMicrotransactionBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.Microtransaction {
	return batch.GetMicrotransactionsBatch().GetItems()
}
