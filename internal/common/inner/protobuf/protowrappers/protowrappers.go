package protowrappers

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

func ProtoSizer[T proto.Message]() batch.Sizer[T] {
	return func(item T) int {
		return proto.Size(item)
	}
}

func WrapTransactions(transactions []*protobuf.Transaction) *protobuf.TransactionBatch {
	return &protobuf.TransactionBatch{
		Transactions: transactions,
	}
}

func WrapMicrotransactions(transactions []*protobuf.Microtransaction) *protobuf.MicrotransactionResult {
	return &protobuf.MicrotransactionResult{
		Transactions: transactions,
	}
}

func WrapToConvertTransactions(transactions []*protobuf.ToConvertTransaction) *protobuf.ToConvertTransactionBatch {
	return &protobuf.ToConvertTransactionBatch{
		Items: transactions,
	}
}

func WrapToMicrotrasactionBatch(transactions []*protobuf.Microtransaction) *protobuf.MicrotransactionBatch {
	return &protobuf.MicrotransactionBatch{
		Items: transactions,
	}
}

func WrapPeriodFilter(items []*protobuf.PeriodFilter) *protobuf.PeriodFilterBatch {
	return &protobuf.PeriodFilterBatch{
		Items: items,
	}
}

func WrapToConvertPeriodFiltered(items []*protobuf.ToConvertPeriodFiltered) *protobuf.ToConvertPeriodFilteredBatch {
	return &protobuf.ToConvertPeriodFilteredBatch{
		Items: items,
	}
}

func WrapToConvertTypeFilteredPayment(items []*protobuf.ToConvertTypeFilteredPayment) *protobuf.ToConvertTypeFilteredPaymentBatch {
	return &protobuf.ToConvertTypeFilteredPaymentBatch{
		Items: items,
	}
}

func WrapAvgByTypeTransactions(items []*protobuf.AvgByTypeTransaction) *protobuf.AvgByTypeTransactionBatch {
	return &protobuf.AvgByTypeTransactionBatch{
		Items: items,
	}
}

func WrapConvertedAmounts(items []*protobuf.ConvertedAmount) *protobuf.ConvertedAmountBatch {
	return &protobuf.ConvertedAmountBatch{
		Items: items,
	}
}

func WrapSuspiciousPaths(paths []*protobuf.SuspiciousPath) *protobuf.SuspiciousPathBatch {
	return &protobuf.SuspiciousPathBatch{
		Paths: paths,
	}
}

func WrapSuspiciousAccounts(accounts []*protobuf.Account) *protobuf.SuspiciousAccountBatch {

	return &protobuf.SuspiciousAccountBatch{
		Accounts: accounts,
	}
}

func WrapMaxBank(maxBank []*protobuf.MaxBank) *protobuf.MaxBankBatch {
	return &protobuf.MaxBankBatch{
		MaxBankMessage: maxBank,
	}
}

func WrapMaxBankResults(results []*protobuf.MaxBankResult) *protobuf.MaxBankResultBatch {
	return &protobuf.MaxBankResultBatch{
		Results: results,
	}
}

func WrapScatterGather(items []*protobuf.ScatterGather) *protobuf.ScatterGatherBatch {
	return &protobuf.ScatterGatherBatch{
		Items: items,
	}
}
