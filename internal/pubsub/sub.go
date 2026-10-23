package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	delivery, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		defer func() {
			if err = ch.Close(); err != nil {
				log.Printf("failed to close channel: %s", err)
			}
		}()

		for d := range delivery {
			var val T

			err = json.Unmarshal(d.Body, &val)
			if err != nil {
				log.Printf("failed to unmarshal message: %s", err)
				continue
			}

			handler(val)
			err = d.Ack(false)
			if err != nil {
				log.Printf("failed to ack message: %s", err)
			}
		}
	}()

	return nil
}
