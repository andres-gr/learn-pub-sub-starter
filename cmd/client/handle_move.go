package main

import (
	"fmt"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
)

func handleMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) {
	defer fmt.Print("> ")

	return func(move gamelogic.ArmyMove) {
		gs.HandleMove(move)
	}
}
