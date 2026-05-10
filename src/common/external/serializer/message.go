package serializer

import (
	"errors"

	"tp-lavado-dinero/common/external/protocol"
)

func SerializeMessage(msg *protocol.Message) ([]byte, error) {
	data := SerializeUint32(uint32(msg.Type))

	data = append(data,
		SerializeString(msg.JobID)...,
	)

	data = append(data,
		SerializeString(msg.SenderID)...,
	)

	data = append(data,
		SerializeBytes(msg.Payload)...,
	)

	return data, nil
}

func DeserializeMessage(data []byte) (*protocol.Message, error) {
	offset := 0

	if offset+UINT32_SIZE > len(data) {
		return nil, errors.New("invalid message type")
	}

	msgType := protocol.MsgType(
		DeserializeUint32(data[offset : offset+UINT32_SIZE]),
	)
	offset += UINT32_SIZE

	switch msgType {
	case protocol.TypeTransaction,
		protocol.TypeEOF,
		protocol.TypeResult:
	default:
		return nil, errors.New("invalid message type")
	}

	jobIDBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	senderIDBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	payloadBytes, err := ReadSizedField(data, &offset)
	if err != nil {
		return nil, err
	}

	if offset != len(data) {
		return nil, errors.New("unexpected trailing bytes")
	}

	return &protocol.Message{
		Type:     msgType,
		JobID:    DeserializeString(jobIDBytes),
		SenderID: DeserializeString(senderIDBytes),
		Payload:  DeserializeBytes(payloadBytes),
	}, nil
}
