package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	conn := "amqp://guest:guest@localhost:5672/"
	rabbit, err := amqp.Dial(conn)
	if err != nil {
		log.Fatal("oops", err)
	}
	defer rabbit.Close()
	fmt.Println("Successfully started the server!")
	gamelogic.PrintServerHelp()

	// Pub
	channel, err := rabbit.Channel()
	if err != nil {
		log.Fatal(err)
	}

	for {
		inputs := gamelogic.GetInput()
		firstInput := inputs[0]
		if firstInput == "pause" {
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: true,
				})
			if err != nil {
				log.Fatal(err)
			}
		} else if firstInput == "resume" {
			err = pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{
					IsPaused: false,
				})
			if err != nil {
				log.Fatal(err)
			}
		} else if firstInput == "quit" {
			fmt.Println("Quitting the game...")
			break
		} else {
			fmt.Println("I don't understand the command")
		}
	}

	// Watch for interrupt
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("RabbitMQ connection closed.")
}
