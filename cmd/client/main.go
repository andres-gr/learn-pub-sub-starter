package main

import (
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

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

	gs := gamelogic.NewGameState(username)
	qName := routing.QueueName(routing.PauseKey, gs.GetUsername())
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilDirect,
		qName,
		routing.PauseKey,
		pubsub.TransientQueueType,
		handlePause(gs),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to %s: %s", qName, err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("failed to open a channel: %s", err)
	}

	armyMovesQ := routing.QueueName(routing.ArmyMovesPrefix, gs.GetUsername())
	armyMovesKey := routing.QueueName(routing.ArmyMovesPrefix, "*")
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		armyMovesQ,
		armyMovesKey,
		pubsub.TransientQueueType,
		handleMove(gs, ch),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to %s: %s", armyMovesQ, err)
	}

	warQ := routing.QueueName(routing.WarRecognitionsPrefix, "*")
	err = pubsub.SubscribeJSON(
		conn,
		routing.ExchangePerilTopic,
		routing.WarRecognitionsPrefix,
		warQ,
		pubsub.DurableQueueType,
		handleWar(gs, ch),
	)
	if err != nil {
		log.Fatalf("failed to subscribe to %s: %s", routing.WarRecognitionsPrefix, err)
	}

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

			err = pubsub.PublishJSON(
				ch,
				routing.ExchangePerilTopic,
				armyMovesQ,
				move,
			)
			if err != nil {
				log.Fatalf("failed to publish move: %s", err)
				return
			}

			log.Printf("[client] move: %v", move)
			log.Print("> ")

		case string(gamelogic.StatusKey):
			gs.CommandStatus()
			log.Print("> ")

		case string(gamelogic.HelpKey):
			gamelogic.PrintClientHelp()

		case string(gamelogic.SpamKey):
			if len(input) < 2 {
				log.Println("[client] spam <n>, where <n> is the number of messages to spam")
				continue
			}

			cnt, err := strconv.Atoi(input[1])
			if err != nil {
				log.Printf("[client] failed to convert %s to int: %s", input[1], err)
				continue
			}

			for range cnt {
				msg := gamelogic.GetMaliciousLog()
				qKey := routing.QueueName(routing.GameLogSlug, gs.GetUsername())
				err := pubsub.PublishGob(
					ch,
					routing.ExchangePerilTopic,
					qKey,
					routing.GameLog{
						CurrentTime: time.Now(),
						Message:     msg,
						Username:    gs.GetUsername(),
					},
				)
				if err != nil {
					log.Fatalf("failed to publish log: %s", err)
				}
			}

			log.Print("> ")

		case routing.QuitKey:
			gamelogic.PrintQuit()

			isActive = false

		default:
			log.Printf("[client] unknown command: %s", input[0])
			gamelogic.PrintClientHelp()
			log.Print("> ")
		}
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	<-sigChan

	log.Println("[client] shutting down")
}
