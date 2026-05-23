package protowrappers

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/batch"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
	"google.golang.org/protobuf/proto"
)

func WrapTransactions(transactions []*protobuf.Transaction) *protobuf.TransactionBatch {
	return &protobuf.TransactionBatch{
		Transactions: transactions,
	}
}

func ProtoSizer[T proto.Message]() batch.Sizer[T] {
	return func(item T) int {
		return proto.Size(item)
	}
}
