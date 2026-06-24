package middleware

import (
	"context"
)

type ExchangeMiddleware struct {
	baseMiddleware
	exchange   string
	keys       []string
	queueName  string
	autoDelete bool
	exclusive  bool
	setup      bool
}

func (em *ExchangeMiddleware) SetUp() error {
	queue, err := em.consumerChannel.QueueDeclare(
		em.queueName,  // name
		false,         // durability
		em.autoDelete, // delete when unused
		em.exclusive,  // exclusive
		false,         // no-wait
		nil,
	)

	if err != nil {
		return ErrMessageMiddlewareMessage
	}
	em.queueName = queue.Name

	for _, key := range em.keys {
		err = em.consumerChannel.QueueBind(
			queue.Name,  // queue name
			key,         // routing key
			em.exchange, // exchange
			false,       // no-wait
			nil,
		)
		if err != nil {
			return ErrMessageMiddlewareMessage
		}
	}

	em.setup = true

	return nil
}

func (em *ExchangeMiddleware) StartConsuming(callbackFunc func(msg Message, ack func(), nack func())) error {
	if !em.setup {
		if err := em.SetUp(); err != nil {
			return err
		}
	}

	msgs, err := em.consumerChannel.Consume(
		em.queueName,   // queue
		em.consumerTag, // consumer
		false,          // auto-ack
		false,          // exclusive
		false,          // no-local
		false,          // no-wait
		nil,            // args
	)
	if err != nil {
		return ErrMessageMiddlewareMessage
	}

	return em.runConsumerLoop(msgs, callbackFunc)
}

func (em *ExchangeMiddleware) StopConsuming() error {
	return em.baseMiddleware.stopConsuming()
}

func (em *ExchangeMiddleware) Send(msg Message) error {
	// se opta por usar un ctx para mantenernos en un tipo limite
	// y seguir la propuesta de RabbitMQ
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	for _, key := range em.keys {
		err := em.publish(ctx, em.exchange, key, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func (em *ExchangeMiddleware) SendWithKey(key string, msg Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), publishTimeout)
	defer cancel()

	return em.publish(ctx, em.exchange, key, msg)
}

func (em *ExchangeMiddleware) Close() error {
	return em.baseMiddleware.close()
}
