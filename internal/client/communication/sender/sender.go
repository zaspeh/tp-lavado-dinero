package sender

import (
	"net"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/external"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/protocol"
	"github.com/zaspeh/tp-lavado-dinero/internal/common/external/serializer"
)

func SendTransaction(
	conn net.Conn,
	jobID string,
	clientID string,
	tx *protocol.Transaction,
) error {
	payload, err := serializer.SerializeTransaction(tx)
	if err != nil {
		return err
	}

	msg := &protocol.Message{
		Type:     protocol.TypeTransaction,
		JobID:    jobID,
		SenderID: clientID,
		Payload:  payload,
	}

	return external.WriteMessage(conn, msg)
}

func SendEOF(
	conn net.Conn,
	jobID string,
	clientID string,
) error {
	payload, err := serializer.SerializeEOF(
		&protocol.EOF{
			JobID:   jobID,
			Source:  clientID,
			QueryID: "client",
		},
	)
	if err != nil {
		return err
	}

	msg := &protocol.Message{
		Type:     protocol.TypeEOF,
		JobID:    jobID,
		SenderID: clientID,
		Payload:  payload,
	}

	return external.WriteMessage(conn, msg)
}
