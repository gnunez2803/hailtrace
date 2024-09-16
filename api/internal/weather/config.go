package weather

import (
	"errors"
	"os"
)

type Kakfa struct {
	Broker        string
	ConsumerTopic string
	GroupId       string
}

func ParseEnv() (Kakfa, error) {
	var kafkaConfig Kakfa
	kafkaConfig.ConsumerTopic = os.Getenv("KAFKA_CONSUMER_TOPIC")
	if kafkaConfig.ConsumerTopic == "" {
		return kafkaConfig, errors.New("kafka consumer topic is not set.")
	}
	kafkaConfig.Broker = os.Getenv("KAFKA_ENDPOINT")
	if kafkaConfig.Broker == "" {
		kafkaConfig.Broker = "localhost:9092"
	}
	kafkaConfig.GroupId = os.Getenv("KAKFA_GROUP_ID")
	if kafkaConfig.GroupId == "" {
		kafkaConfig.GroupId = "go-weather-etl"
	}
	return kafkaConfig, nil
}
