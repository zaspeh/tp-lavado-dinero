package protoextractors

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"

func GetConvertedAmountBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.ConvertedAmount {
	return batch.GetConvertedAmountBatch().GetItems()
}
