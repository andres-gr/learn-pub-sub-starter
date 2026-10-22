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

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %s", err)
	}

	log.Printf("[server] connected to %s", queueServer)

	qKey := routing.QueueName(routing.GameLogSlug, "*")
	_, _, err = pubsub.DeclareAndBind(
		conn,
		routing.ExchangePerilTopic,
		routing.GameLogSlug,
		qKey,
		pubsub.DurableQueueType,
	)
	if err != nil {
		log.Fatalf("failed to declare and bind queue: %s", err)
	}

	gamelogic.PrintClientHelp()

	for isActive := true; isActive; {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case routing.PauseKey:
			log.Println("[server] pausing game")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				},
			)
			if err != nil {
				log.Fatalf("failed to publish JSON: %s", err)
			}

		case routing.ResumeKey:
			log.Println("[server] resuming game")

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				},
			)
			if err != nil {
				log.Fatalf("failed to publish JSON: %s", err)
			}

		case routing.QuitKey:
			log.Println("[server] exiting")
			isActive = false

		default:
			log.Printf("[server] unknown command: %s", input[0])
			gamelogic.PrintClientHelp()
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	<-sigChan

	log.Println("[server] shutting down")
}
