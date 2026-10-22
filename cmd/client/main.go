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

	gs := gamelogic.NewGameState(username)

	for isActive := true; isActive; {
		input := gamelogic.GetInput()
		if len(input) == 0 {
			continue
		}

		switch input[0] {
		case string(gamelogic.SpawnKey):
			err = gs.CommandSpawn(input)
			if err != nil {
				log.Fatalf("failed to spawn: %s", err)
			}

		case string(gamelogic.MoveKey):
			move, err := gs.CommandMove(input)
			if err != nil {
				log.Fatalf("failed to move: %s", err)
				return
			}

			log.Printf("[client] move: %v", move)

		case string(gamelogic.StatusKey):
			gs.CommandStatus()

		case string(gamelogic.HelpKey):
			gamelogic.PrintClientHelp()

		case string(gamelogic.SpamKey):
			log.Println("[client] Spamming not allowed yet!")

		case routing.QuitKey:
			gamelogic.PrintQuit()

			isActive = false

		default:
			log.Printf("[client] unknown command: %s", input[0])
			gamelogic.PrintClientHelp()
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	<-sigChan

	log.Println("[client] shutting down")
}
