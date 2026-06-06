package result

import protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"

type MicrotransactionResult struct {
	Transactions []*protobuf.Microtransaction
}

func (m MicrotransactionResult) Handle(handler ResultHandler) error {
	return handler.HandleMicrotransactionResult(m)
}
