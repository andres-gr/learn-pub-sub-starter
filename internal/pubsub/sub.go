package pubsub

import (
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack         AckType = "Ack"
	NackDiscard AckType = "NackDiscard"
	NackRequeue AckType = "NackRequeue"
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	ch, q, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		nil,
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

			ackType := handler(val)
			switch ackType {
			case Ack:
				err = d.Ack(false)
				if err != nil {
					log.Printf("failed to ack message: %s", err)
				}

			case NackDiscard:
				err = d.Nack(false, false)
				if err != nil {
					log.Printf("failed to nack message: %s", err)
				}

			case NackRequeue:
				err = d.Nack(false, true)
				if err != nil {
					log.Printf("failed to nack message: %s", err)
				}
			}
		}
	}()

	return nil
}
