// Package routing contains constants for routing keys and exchange names.
package routing

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
