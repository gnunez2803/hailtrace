package storm

import (
	"context"
	"errors"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

type Process struct {
	Consumer      *kafka.Consumer
	Producer      *kafka.Producer
	ProducerTopic string
}

func InitProcess() (Process, error) {
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
	producer, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": config.Broker})
	if err != nil {
		return Process{}, errors.New("Unable to create kakfa producer")
	}

	return Process{
		Consumer:      consumer,
		Producer:      producer,
		ProducerTopic: config.ProducerTopic,
	}, nil
}

func (p Process) Start(ctx context.Context) error {
	defer p.Consumer.Close()
	defer p.Producer.Close()

	log.Println("Service is running... Listening for messages...")
	messageChan := make(chan *kafka.Message, 100) // Buffered channel for incoming messages

	// Goroutine to consume messages and send them to the channel
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				msg, err := p.Consumer.ReadMessage(100 * time.Millisecond)
				if err != nil {
					if kafkaErr, ok := err.(kafka.Error); ok && kafkaErr.Code() == kafka.ErrTimedOut {
						continue
					}
					log.Printf("Error consuming message: %v\n", err)
					continue
				}
				messageChan <- msg
			}
		}
	}()

	// Goroutine to process messages from the channel
	go func() {
		for msg := range messageChan {
			if err := handleMessage(p.Producer, msg, p.ProducerTopic); err != nil {
				log.Printf("Error processing message: %v\n", err)
			}
		}
	}()

	// Block until context is canceled
	<-ctx.Done()
	log.Println("Received shutdown signal. Stopping service.")
	return nil
}
