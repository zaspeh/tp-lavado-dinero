package result

import "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"

type MicrotransactionResult struct {
	Transactions []*protobuf.Microtransaction
}

func (m MicrotransactionResult) Handle(handler ResultHandler) error {
	return handler.HandleMicrotransactionResult(m)
}
