package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func handlePause(gs *gamelogic.GameState) func(routing.PlayingState) {
	defer fmt.Print("> ")

	return func(state routing.PlayingState) {
		gs.HandlePause(state)
	}
}
