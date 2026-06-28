package leader

import (
	"log/slog"
	"time"
)

func (c *BullyCoordinator) runTicker() {
	ticker := time.NewTicker(c.heartbeatInterval)
	defer ticker.Stop()
	time.AfterFunc(c.leaderTimeout, c.startElection)

	c.mu.Lock()
	c.lastLeaderSeen = time.Now()
	c.mu.Unlock()

	for range ticker.C {
		c.mu.Lock()

		if c.closed {
			c.mu.Unlock()
			return
		}

		leaderID := c.leaderID
		knownLeader := c.leaderID != -1
		leaderTimedOut :=
			knownLeader &&
				time.Since(c.lastLeaderSeen) > c.leaderTimeout
		electionInProgress := c.electionInProgress
		c.mu.Unlock()

		if leaderID == c.id {
			slog.Info("sending heartbeat")
			if err := c.broadcastCoordinator(); err != nil {
				slog.Warn(
					"failed to broadcast coordinator heartbeat",
					"error",
					err,
				)
			}
			continue
		}

		if leaderTimedOut && !electionInProgress {
			c.startElection()
		}
	}
}

func (c *BullyCoordinator) startElection() {
	c.mu.Lock()

	if c.leaderID == c.id {
		c.mu.Unlock()
		return
	}

	if c.closed || c.electionInProgress {
		c.mu.Unlock()
		return
	}

	c.electionInProgress = true
	c.gotAnswer = false

	c.mu.Unlock()

	slog.Info(
		"starting bully election",
		"hypervisor_id",
		c.id,
	)

	higherNodes := 0

	for peerID := c.id + 1; peerID < c.count; peerID++ {
		higherNodes++

		if err := c.sendElection(peerID); err != nil {
			slog.Warn(
				"failed to send election",
				"peer_id",
				peerID,
				"error",
				err,
			)
		}
	}

	if higherNodes == 0 {
		c.becomeLeader()
		return
	}

	time.AfterFunc(
		c.electionTimeout,
		c.finishElectionIfUnanswered,
	)
}

func (c *BullyCoordinator) finishElectionIfUnanswered() {
	c.mu.Lock()

	if c.closed ||
		!c.electionInProgress ||
		c.gotAnswer {
		c.mu.Unlock()
		return
	}

	c.mu.Unlock()

	c.becomeLeader()
}

func (c *BullyCoordinator) becomeLeader() {
	c.mu.Lock()

	if c.closed {
		c.mu.Unlock()
		return
	}

	if c.leaderID == c.id {
		c.mu.Unlock()
		return
	}

	c.leaderID = c.id
	c.lastLeaderSeen = time.Now()
	c.electionInProgress = false
	c.gotAnswer = false

	c.mu.Unlock()

	slog.Info(
		"became leader",
		"hypervisor_id",
		c.id,
	)

	if err := c.handler.BecomeLeader(); err != nil {
		slog.Error(
			"failed to become leader",
			"error",
			err,
		)
	}

	c.mu.Lock()
	c.lastLeaderSeen = time.Now()
	c.mu.Unlock()

	_ = c.broadcastCoordinator()
}
