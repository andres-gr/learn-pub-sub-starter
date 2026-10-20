package main

import (
	"log"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func main() {
	const queueServer = "amqp://guest:guest@localhost:5672/"

	conn, err := amqp.Dial(queueServer)
	if err != nil {
		log.Fatalf("failed to connect to RabbitMQ: %s", err)
	}

	defer func() {
		if err = conn.Close(); err != nil {
			log.Fatalf("failed to close connection: %s", err)
		}
	}()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("failed to get username: %s", err)
	}

	qName := routing.QueueName(routing.PauseKey, username)
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilDirect,
		qName,
		routing.PauseKey,
		pubsub.TransientQueueType,
	)
	if err != nil {
		log.Fatalf("failed to declare and bind queue: %s", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	<-sigChan

	log.Println("[client] shutting down")
}
