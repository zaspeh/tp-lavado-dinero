package message

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"

type MicrotransactionResult struct {
	Transactions []*protobuf.Microtransaction
}

func (MicrotransactionResult) IsResult() {}
