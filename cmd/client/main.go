package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	// Connect
	conn := "amqp://guest:guest@localhost:5672/"
	rabbit, err := amqp.Dial(conn)
	if err != nil {
		log.Fatal("not good!", err)
	}
	defer rabbit.Close()
	fmt.Println("Connected to the server!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal("bad username", err)
	}
	gameState := gamelogic.NewGameState(username)

	err = pubsub.SubscribeJSON(
		rabbit,
		routing.ExchangePerilDirect,
		"pause."+username, routing.PauseKey,
		pubsub.TransientQueueType,
		handlerPause(gameState),
	)
	if err != nil {
		log.Fatal("no chan or queue womp womp", err)
	}

gameloop:
	for {
		defer fmt.Print("> ")
		inputs := gamelogic.GetInput()
		if len(inputs) == 0 {
			fmt.Println("No valid input, try again")
			continue
		}

		command := inputs[0]
		switch command {
		case "spawn":
			err = gameState.CommandSpawn(inputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
		case "move":
			_, err = gameState.CommandMove(inputs)
			if err != nil {
				fmt.Println(err)
				continue
			}
			fmt.Println("move successful!")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet")
		case "quit":
			gamelogic.PrintQuit()
			break gameloop
		default:
			fmt.Println("No valid input, try again")
			continue
		}
	}
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) {
	return func(ps routing.PlayingState) {
		defer fmt.Println("> ")
		gs.HandlePause(
			ps,
		)
	}
}
