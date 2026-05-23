package protobuf

import (
	"fmt"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"google.golang.org/protobuf/proto"
)

func SerializeProtoMessageONTRIAL(clientID string, messageType MessageType, innerMessage isMoneyLaundry_InnerMessage) (middleware.Message, error) {
	moneyLaundering := &MoneyLaundry{
		ClientID:     clientID,
		Type:         messageType,
		InnerMessage: innerMessage,
	}
	return serializeMoneyLaundering(moneyLaundering)
}

func serializeMoneyLaundering(moneyLaundering *MoneyLaundry) (middleware.Message, error) {
	marshalledMoneyLaundering, err := proto.Marshal(moneyLaundering)
	if err != nil {
		return middleware.Message{}, fmt.Errorf("error marshalling money laundry: %w", err)
	}

	return middleware.Message{
		Body: marshalledMoneyLaundering,
	}, nil
}
