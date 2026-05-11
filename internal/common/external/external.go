package external

import (
	"io"

	"tp-lavado-dinero/common/external/protocol"
	"tp-lavado-dinero/common/external/serializer"
	"tp-lavado-dinero/common/external/socket"
)

func WriteMessage(writer io.Writer, msg *protocol.Message) error {
	data, err := serializer.SerializeMessage(msg)
	if err != nil {
		return err
	}

	return socket.Write(writer, data)
}

func ReadMessage(reader io.Reader) (*protocol.Message, error) {
	data, err := socket.Read(reader)
	if err != nil {
		return nil, err
	}

	return serializer.DeserializeMessage(data)
}
