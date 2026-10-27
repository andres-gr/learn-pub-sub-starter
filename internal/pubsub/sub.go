package pubsub

import (
	"bytes"
	"encoding/gob"
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

func unmarshalJSON[T any](b []byte) (T, error) {
	var val T
	err := json.Unmarshal(b, &val)
	return val, err
}

func unmarshalGob[T any](b []byte) (T, error) {
	var val T
	dec := gob.NewDecoder(bytes.NewReader(b))
	err := dec.Decode(&val)
	return val, err
}

func subscribe[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
	unmarshaller func([]byte) (T, error),
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

	err = ch.Qos(10, 0, false)
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
			val, err := unmarshaller(d.Body)
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

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshalJSON,
	)
	if err != nil {
		return err
	}

	return nil
}

func SubscribeGob[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	err := subscribe(
		conn,
		exchange,
		queueName,
		key,
		queueType,
		handler,
		unmarshalGob,
	)
	if err != nil {
		return err
	}

	return nil
}
