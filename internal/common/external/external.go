package external

import (
	"io"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/socket"
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
