package heartbeat

import (
	"log/slog"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
	protobuf "github.com/zaspeh/tp-lavado-dinero/internal/common/inner/protobuf/protomessages"
)

type HeartbeatPublisher struct {
	queue      middleware.Middleware
	workerID   int
	workerType string
	interval   time.Duration
	stop       chan struct{}
}

func NewHeartbeatPublisher(queue middleware.Middleware, workerID int, workerType string, intervalSeconds int) *HeartbeatPublisher {
	return &HeartbeatPublisher{
		queue:      queue,
		workerID:   workerID,
		workerType: workerType,
		interval:   time.Duration(intervalSeconds) * time.Second,
		stop:       make(chan struct{}),
	}
}

func (p *HeartbeatPublisher) Run() {
	p.publishHeartbeat()

	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			p.publishHeartbeat()

		case <-p.stop:
			return
		}
	}
}

func (p *HeartbeatPublisher) publishHeartbeat() {
	slog.Debug(
		"heartbeat sent",
		"worker_id", p.workerID,
		"worker_type", p.workerType,
	)
	msg, err := protobuf.SerializeProtoHeartbeatONTRIAL(
		int64(p.workerID),
		p.workerType,
	)

	if err != nil {
		slog.Error("failed to serialize heartbeat", "err", err)
		return
	}

	if err := p.queue.Send(msg); err != nil {
		slog.Error("failed to send heartbeat", "err", err)
		return
	}
}

func (p *HeartbeatPublisher) Close() error {
	close(p.stop)
	return p.queue.Close()
}
