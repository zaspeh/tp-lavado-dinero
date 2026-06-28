package leader

import (
	"log/slog"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

func (c *BullyCoordinator) handleMessage(
	msg middleware.Message,
	ack,
	nack func(),
) {
	moneyLaundry, err := protobuf.DeserializeMoneyLaunderingONTRIAL(msg)
	if err != nil {
		nack()
		return
	}

	switch moneyLaundry.GetType() {

	case protobuf.MessageType_COORDINATOR_ELECTION:
		c.handleElection(
			int(
				moneyLaundry.GetCoordinatorElection().GetSenderId(),
			),
		)

	case protobuf.MessageType_COORDINATOR_OK:
		c.handleOK(
			int(
				moneyLaundry.GetCoordinatorOk().GetSenderId(),
			),
		)

	case protobuf.MessageType_COORDINATOR_HEARTBEAT:
		c.handleCoordinator(
			int(
				moneyLaundry.GetCoordinatorHeartbeat().GetLeaderId(),
			),
		)

	default:
		slog.Warn(
			"unknown coordinator message",
			"type",
			moneyLaundry.GetType(),
		)
	}

	ack()
}

func (c *BullyCoordinator) handleElection(senderID int) {
	c.mu.Lock()
	alreadyLeader := c.leaderID == c.id
	c.mu.Unlock()

	if alreadyLeader {
		if err := c.sendOK(senderID); err != nil {
			slog.Warn(
				"failed sending OK",
				"peer_id",
				senderID,
				"error",
				err,
			)
		}
		return
	}

	if senderID >= c.id {
		slog.Info("Election message", "leaderId", senderID)
		return
	}

	slog.Info(
		"received election",
		"from",
		senderID,
	)

	if err := c.sendOK(senderID); err != nil {
		slog.Warn(
			"failed sending OK",
			"peer_id",
			senderID,
			"error",
			err,
		)
	}

	c.startElection()
}

func (c *BullyCoordinator) handleOK(senderID int) {

	c.mu.Lock()
	defer c.mu.Unlock()

	c.gotAnswer = true

	slog.Info(
		"received OK",
		"from",
		senderID,
	)
}

func (c *BullyCoordinator) handleCoordinator(leaderID int) {

	c.mu.Lock()

	firstLeader := c.leaderID == -1

	c.leaderID = leaderID
	c.lastLeaderSeen = time.Now()
	c.electionInProgress = false
	c.gotAnswer = false

	c.mu.Unlock()

	if firstLeader {
		if err := c.handler.MarkReady(); err != nil {
			slog.Error(
				"failed to mark cluster ready",
				"error",
				err,
			)
		}
	}

	slog.Debug(
		"leader heartbeat",
		"leader",
		leaderID,
	)
}
