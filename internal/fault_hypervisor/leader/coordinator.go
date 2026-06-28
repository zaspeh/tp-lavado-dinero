package leader

import (
	"sync"
	"time"

	"github.com/zaspeh/tp-lavado-dinero/internal/common/inner/middleware"
)

type LeadershipHandler interface {
	BecomeLeader() error
	MarkReady() error
}

type Coordinator interface {
	Run() error
	Close() error
}

type BullyConfig struct {
	ID                 int
	Count              int
	ExchangeName       string
	ConnectionSettings middleware.ConnSettings
	HeartbeatInterval  time.Duration
	LeaderTimeout      time.Duration
	ElectionTimeout    time.Duration
}

type BullyCoordinator struct {
	id           int
	count        int
	exchangeName string

	exchange *middleware.ExchangeMiddleware
	handler  LeadershipHandler

	heartbeatInterval time.Duration
	leaderTimeout     time.Duration
	electionTimeout   time.Duration

	mu sync.Mutex

	leaderID           int
	lastLeaderSeen     time.Time
	electionInProgress bool
	gotAnswer          bool
	closed             bool
}

func NewBullyCoordinator(
	config BullyConfig,
	handler LeadershipHandler,
) (*BullyCoordinator, error) {

	if config.HeartbeatInterval == 0 {
		config.HeartbeatInterval = 2 * time.Second
	}

	if config.LeaderTimeout == 0 {
		config.LeaderTimeout = 6 * time.Second
	}

	if config.ElectionTimeout == 0 {
		config.ElectionTimeout = 3 * time.Second
	}

	var exchange *middleware.ExchangeMiddleware

	if config.Count > 1 {
		var err error

		exchange, err = middleware.CreateExchangeMiddleware(
			config.ExchangeName,
			[]string{routingKey(config.ID)},
			config.ConnectionSettings,
			false,
			false,
			"",
			"fault_hypervisor",
		)

		if err != nil {
			return nil, err
		}
	}

	return &BullyCoordinator{
		id:                config.ID,
		count:             config.Count,
		exchangeName:      config.ExchangeName,
		exchange:          exchange,
		handler:           handler,
		heartbeatInterval: config.HeartbeatInterval,
		leaderTimeout:     config.LeaderTimeout,
		electionTimeout:   config.ElectionTimeout,
		leaderID:          -1,
	}, nil
}

func (c *BullyCoordinator) Run() error {
	if c.count == 1 {
		return c.handler.BecomeLeader()
	}

	if err := c.exchange.SetUp(); err != nil {
		return err
	}

	go c.runTicker()

	return c.exchange.StartConsuming(
		func(
			msg middleware.Message,
			ack,
			nack func(),
		) {
			c.handleMessage(msg, ack, nack)
		},
	)
}

func (c *BullyCoordinator) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()

	if c.exchange == nil {
		return nil
	}

	return c.exchange.Close()
}
