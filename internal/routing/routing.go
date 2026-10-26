// Package routing contains constants for routing keys and exchange names.
package routing

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
)

const (
	ArmyMovesPrefix = "army_moves"

	WarRecognitionsPrefix = "war"

	PauseKey = "pause"

	ResumeKey = "resume"

	QuitKey = "quit"

	GameLogSlug = "game_logs"
)

const (
	ExchangePerilDirect = "peril_direct"
	ExchangePerilTopic  = "peril_topic"
)

func QueueName(key, name string) string {
	if name == "" {
		return key
	}

	return key + "." + name
}

func PublishGameLog(attacker, msg string, ch *amqp.Channel) error {
	key := QueueName(GameLogSlug, attacker)
	log := GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    attacker,
	}

	err := pubsub.PublishGob(
		ch,
		ExchangePerilTopic,
		key,
		log,
	)
	if err != nil {
		return err
	}

	return nil
}
