package protoinserter

import (
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf"
)

func InsertConvertedAmountBatch(clientID string, batch *protobuf.ConvertedAmountBatch) (middleware.Message, error) {
	innerMessage := &protobuf.MoneyLaundry_ConvertedAmountBatch{
		ConvertedAmountBatch: batch,
	}
	return protobuf.SerializeProtoMessageONTRIAL(
		clientID,
		protobuf.MessageType_CONVERTED_AMOUNT_BATCH,
		innerMessage,
	)
}
