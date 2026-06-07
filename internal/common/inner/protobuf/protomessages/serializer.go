package protobuf

import (
	"fmt"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	"google.golang.org/protobuf/proto"
)

func SerializeProtoMessageONTRIAL(clientID string, messageType MessageType, innerMessage isMoneyLaundry_InnerMessage) (middleware.Message, error) {
	moneyLaundering := &MoneyLaundry{
		ClientID: clientID,
		Type:     messageType,
	}
	if innerMessage != nil {
		moneyLaundering.InnerMessage = innerMessage
	}
	return serializeMoneyLaundering(moneyLaundering)
}

func SerializeProtoHeartbeatONTRIAL(WorkerID int64, workerType string) (middleware.Message, error) {
	heartbeatMessage := &MoneyLaundry{
		Type: MessageType_HEARTBEAT,
		InnerMessage: &MoneyLaundry_Heartbeat{
			Heartbeat: &Heartbeat{
				WorkerId:   WorkerID,
				WorkerType: workerType,
				Timestamp:  time.Now().Unix(),
			},
		},
	}

	return serializeMoneyLaundering(heartbeatMessage)
}

func DeserializeMoneyLaunderingONTRIAL(msg middleware.Message) (*MoneyLaundry, error) {
	var moneyLaundering MoneyLaundry
	err := proto.Unmarshal(msg.Body, &moneyLaundering)
	if err != nil {
		return nil, fmt.Errorf("error unmarshalling money laundry: %w", err)
	}

	return &moneyLaundering, nil
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
