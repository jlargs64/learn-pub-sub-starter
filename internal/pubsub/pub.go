// Package pubsub handles rabbitmq pubsub
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

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

	err = channel.QueueBind(
		queue.Name,
		key,
		exchange,
		false,
		nil,
	)
	if err != nil {
		return nil, amqp.Queue{}, err
	}
	return channel, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T),
) error {
	subChan, queue, err := DeclareAndBind(
		conn,
		exchange,
		queueName,
		key,
		queueType,
	)
	if err != nil {
		return err
	}

	deliveryChan, err := subChan.Consume(
		queue.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for delivery := range deliveryChan {
			var body T
			if err := json.Unmarshal(delivery.Body, &body); err != nil {
				log.Println("warning err in sub:", err)
			}

			handler(body)

			if err := delivery.Ack(false); err != nil {
				log.Println("warning err in sub:", err)
			}
		}
	}()

	return nil
}
