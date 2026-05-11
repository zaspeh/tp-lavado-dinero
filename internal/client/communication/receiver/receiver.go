package receiver

import (
	"os"
	"path/filepath"

	"net"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
)

func ReceiveResults(
	conn net.Conn,
	outputDir string,
) error {
	msg, err := external.ReadMessage(conn)
	if err != nil {
		return err
	}

	if msg.Type != protocol.TypeResult {
		return errUnexpectedMessageType()
	}

	result, err := serializer.DeserializeResult(
		msg.Payload,
	)
	if err != nil {
		return err
	}

	outputPath := filepath.Join(
		outputDir,
		result.Query+".txt",
	)

	return os.WriteFile(
		outputPath,
		result.Data,
		0644,
	)
}

func errUnexpectedMessageType() error {
	return &unexpectedMessageTypeError{}
}

type unexpectedMessageTypeError struct{}

func (e *unexpectedMessageTypeError) Error() string {
	return "unexpected message type"
}
