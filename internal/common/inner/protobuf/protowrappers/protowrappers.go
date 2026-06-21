package protowrappers

import (
	"strconv"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
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

func WrapTransactionToMicroTransactionBatch(transactions []*protobuf.Transaction) *protobuf.MicrotransactionBatch {
	microtransactions := make([]*protobuf.Microtransaction, len(transactions))
	for i, transaction := range transactions {
		parsedAmount, err := strconv.ParseFloat(transaction.GetAmountPaid(), 64)
		if err != nil {
			continue
		}
		microtransaction := &protobuf.Microtransaction{
			Account:   transaction.GetAccount(),
			ToAccount: transaction.GetToAccount(),
			Amount:    parsedAmount,
		}
		microtransactions[i] = microtransaction
	}
	return WrapToMicrotransactionBatch(microtransactions)
}

func WrapTransactionToMaxBankBatch(transactions []*protobuf.Transaction) *protobuf.MaxBankBatch {
	maxBankMessages := make([]*protobuf.MaxBank, len(transactions))
	for i, transaction := range transactions {
		transferSummary := &protobuf.TransferSummary{
			Account: transaction.GetAccount(),
			Amount:  transaction.GetAmountPaid(),
		}

		maxbank := &protobuf.MaxBank{
			FromBank: transaction.GetFromBank(),
			Payload: &protobuf.MaxBank_TransferSummary{
				TransferSummary: transferSummary,
			},
		}
		maxBankMessages[i] = maxbank
	}

	return WrapMaxBank(maxBankMessages)
}

func WrapTransactionToPeriodFilterBatch(transactions []*protobuf.Transaction) *protobuf.PeriodFilterBatch {
	periodFilterList := make([]*protobuf.PeriodFilter, len(transactions))
	for i, transaction := range transactions {
		periodFilter := &protobuf.PeriodFilter{
			Timestamp:     transaction.GetTimestamp(),
			FromBank:      transaction.GetFromBank(),
			ToBank:        transaction.GetToBank(),
			Account:       transaction.GetAccount(),
			ToAccount:     transaction.GetToAccount(),
			AmountPaid:    transaction.GetAmountPaid(),
			PaymentFormat: transaction.GetPaymentFormat(),
		}
		periodFilterList[i] = periodFilter
	}
	return WrapPeriodFilter(periodFilterList)
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

func WrapToMicrotransactionBatch(transactions []*protobuf.Microtransaction) *protobuf.MicrotransactionBatch {
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

func WrapAvgByTypeResults(items []*protobuf.AvgByTypeResult) *protobuf.AvgByTypeResultBatch {
	return &protobuf.AvgByTypeResultBatch{
		Results: items,
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

func WrapGroupedAccounts(items []*protobuf.GroupedAccounts) *protobuf.GroupedAccountsBatch {
	return &protobuf.GroupedAccountsBatch{
		Groups: items,
	}
}

func WrapIntermediaryPair(items []*protobuf.IntermediaryPair) *protobuf.IntermediaryPairBatch {
	return &protobuf.IntermediaryPairBatch{
		Items: items,
	}
}

func WrapToSuspiciousAccountBatch(accounts []*protobuf.Account) *protobuf.SuspiciousAccountBatch {
	return &protobuf.SuspiciousAccountBatch{
		Accounts: accounts,
	}
}

func WrapToConvertedMicropaymentResultBatch(results []*protobuf.ConvertedMicroPaymentResult) *protobuf.ConvertedMicroPaymentResultBatch {
	return &protobuf.ConvertedMicroPaymentResultBatch{
		Results: results,
	}
}
