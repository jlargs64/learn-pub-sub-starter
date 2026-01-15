// Package pubsub handles rabbitmq pubsub
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	ctx := context.Background()
	var mandatory, immediate bool
	mandatory = false
	immediate = false

	msgBody, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("could not marshal json in publish: %w", err)
	}

	err = ch.PublishWithContext(
		ctx,
		exchange,
		key,
		mandatory,
		immediate,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        msgBody,
		})

	return err
}
