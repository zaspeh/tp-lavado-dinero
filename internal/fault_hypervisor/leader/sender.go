package leader

import (
	"fmt"
	"log/slog"

	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

func (c *BullyCoordinator) sendElection(peerID int) error {
	slog.Info(
		"SEND election",
		"from", c.id,
		"to", peerID,
	)
	msg, err := protobuf.SerializeProtoMessageONTRIAL(
		"",
		protobuf.MessageType_COORDINATOR_ELECTION,
		&protobuf.MoneyLaundry_CoordinatorElection{
			CoordinatorElection: &protobuf.CoordinatorElection{
				SenderId: int32(c.id),
			},
		},
		"",
	)
	if err != nil {
		return err
	}

	return c.exchange.SendWithKey(
		routingKey(peerID),
		msg,
	)
}

func (c *BullyCoordinator) sendOK(peerID int) error {
	slog.Info(
		"SEND ok",
		"from", c.id,
		"to", peerID,
	)
	msg, err := protobuf.SerializeProtoMessageONTRIAL(
		"",
		protobuf.MessageType_COORDINATOR_OK,
		&protobuf.MoneyLaundry_CoordinatorOk{
			CoordinatorOk: &protobuf.CoordinatorOK{
				SenderId: int32(c.id),
			},
		},
		"",
	)
	if err != nil {
		return err
	}

	return c.exchange.SendWithKey(
		routingKey(peerID),
		msg,
	)
}

func (c *BullyCoordinator) broadcastCoordinator() error {
	slog.Info(
		"SEND coordinator",
		"leader", c.id,
	)
	msg, err := protobuf.SerializeProtoMessageONTRIAL(
		"",
		protobuf.MessageType_COORDINATOR_HEARTBEAT,
		&protobuf.MoneyLaundry_CoordinatorHeartbeat{
			CoordinatorHeartbeat: &protobuf.CoordinatorHeartbeat{
				LeaderId: int32(c.id),
			},
		},
		"",
	)
	if err != nil {
		return err
	}

	for peerID := 0; peerID < c.count; peerID++ {
		if peerID == c.id {
			continue
		}

		if err := c.exchange.SendWithKey(
			routingKey(peerID),
			msg,
		); err != nil {
			return err
		}
	}

	return nil
}

func routingKey(id int) string {
	return fmt.Sprintf("hypervisor.%d", id)
}
