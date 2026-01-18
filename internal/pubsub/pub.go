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

type SimpleQueueType string

const (
	DurableQueueType   SimpleQueueType = "durable"
	TransientQueueType SimpleQueueType = "transient"
)

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	shouldAutoDelete := false
	isDurable := false
	isExclusive := false

	if queueType == DurableQueueType {
		isDurable = true
	}
	if queueType == TransientQueueType {
		shouldAutoDelete = true
		isExclusive = true
	}

	queue, err := channel.QueueDeclare(
		queueName,
		isDurable,
		shouldAutoDelete,
		isExclusive,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return channel, queue, nil
}
