package main

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func warLogMsg(winner, loser string, draw bool) string {
	if !draw {
		return fmt.Sprintf("%s won a war against %s", winner, loser)
	}

	return fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
}

func handleWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(war gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")

		out, winner, loser := gs.HandleWar(war)

		switch out {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeOpponentWon:
		case gamelogic.WarOutcomeYouWon:
			msg := warLogMsg(winner, loser, false)
			err := routing.PublishGameLog(war.Attacker.Username, msg, ch)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			msg := warLogMsg(winner, loser, true)
			err := routing.PublishGameLog(war.Attacker.Username, msg, ch)
			if err != nil {
				return pubsub.NackRequeue
			}

			return pubsub.Ack
		}

		return pubsub.NackDiscard
	}
}
