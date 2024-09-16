package weather

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Process struct {
	Consumer *kafka.Consumer
	MRepo    ModelsRepo
}

func InitProcess(mRepo ModelsRepo) (Process, error) {
	config, err := ParseEnv()
	if err != nil {
		return Process{}, err
	}
	// Initialize Kafka consumer
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": config.Broker,
		"group.id":          config.GroupId,
		"auto.offset.reset": "smallest"})
	if err != nil {
		return Process{}, errors.New("Unable to create kakfa consumer")
	}

	// Subscribe to the raw weather data topic
	if err := consumer.Subscribe(config.ConsumerTopic, nil); err != nil {
		return Process{}, errors.New("Unable to subscribed to " + config.ConsumerTopic + " topic")
	}

	return Process{
		Consumer: consumer,
		MRepo:    mRepo,
	}, nil
}

func (p Process) Start(ctx context.Context) error {
	defer p.Consumer.Close()

	log.Println("Service is running... Listening for messages...")
	// Continuously consume and process messages until context is canceled
	for {
		select {
		case <-ctx.Done():
			log.Println("Received shutdown signal. Stopping service.")
			return nil
		default:
			// Read a message from Kafka
			msg, err := p.Consumer.ReadMessage(100 * time.Millisecond)
			if err != nil {
				// Ignore timeout errors or handle specific error types, log others
				if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
					continue
				}
				log.Printf("Error consuming message: %v\n", err)
				continue
			}

			// Process the message
			if err := p.HandleMessage(msg); err != nil {
				log.Printf("Error processing message: %v\n", err)
			}
		}
	}
}
