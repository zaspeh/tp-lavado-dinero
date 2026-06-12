package protoextractors

import protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"

func GetTransactionBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.Transaction {
	return batch.GetTransactions().GetTransactions()
}

func GetToConvertTransactionBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.ToConvertTransaction {
	return batch.GetToConvertBatch().GetItems()
}

func GetPeriodFilterBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.PeriodFilter {
	return batch.GetPeriodFilterBatch().GetItems()
}

func GetConvertedAmountBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.ConvertedAmount {
	return batch.GetConvertedAmountBatch().GetItems()
}

func GetMaxBankBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.MaxBank {
	return batch.GetMaxBankBatch().GetMaxBankMessage()
}

func GetMicrotransactionBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.Microtransaction {
	return batch.GetMicrotransactionsBatch().GetItems()
}

func GetAvgByTypeTransactionBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.AvgByTypeTransaction {
	return batch.GetAvgbytypeTransactionBatch().GetItems()
}

func GetToConvertPeriodFilteredItems(batch *protobuf.MoneyLaundry) []*protobuf.ToConvertPeriodFiltered {
	return batch.GetToConvertPeriodFilteredBatch().GetItems()
}

func GetToConvertTypeFilteredPaymentItems(batch *protobuf.MoneyLaundry) []*protobuf.ToConvertTypeFilteredPayment {
	return batch.GetToConvertTypeFilteredPaymentBatch().GetItems()
}

func GetScatterGatherBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.ScatterGather {
	return batch.GetScattergatherBatch().GetItems()
}

func GetGroupedAccountsBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.GroupedAccounts {
	return batch.GetGroupedAccountsBatch().GetGroups()
}

func GetMaxBankResultBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.MaxBankResult {
	return batch.GetMaxBankResultBatch().GetResults()
}

func GetAvgByTypeResultBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.AvgByTypeResult {
	return batch.GetAvgbytypeResultBatch().GetResults()
}

func GetSuspiciousPathBatchItems(batch *protobuf.MoneyLaundry) []*protobuf.SuspiciousPath {
	return batch.GetSuspiciouspathBatch().GetPaths()
}
