package weather

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func determineStormData(sd MsgData) (WeatherDbEvent, error) {
	switch sd.Type {
	case "wind":
		windEvent := WindEvent{
			Comments: sd.Comments,
			Location: sd.Location,
			State:    sd.State,
			County:   sd.County,
			Lat:      sd.Lat,
			Lon:      sd.Lon,
			Speed:    *sd.Speed,
		}
		eventTime := time.Unix(sd.EventTs, 0).UTC()
		windEvent.EventTime = eventTime
		return windEvent, nil
	case "tornado":
		tornadoEvent := TornadoEvent{
			Comments: sd.Comments,
			Location: sd.Location,
			State:    sd.State,
			County:   sd.County,
			Lat:      sd.Lat,
			Lon:      sd.Lon,
			FScale:   *sd.FScale,
		}
		eventTime := time.Unix(sd.EventTs, 0).UTC()
		tornadoEvent.EventTime = eventTime
		return tornadoEvent, nil
	case "hail":
		hailEvent := HailEvent{
			Comments: sd.Comments,
			Location: sd.Location,

			State:  sd.State,
			County: sd.County,
			Lat:    sd.Lat,
			Lon:    sd.Lon,
			Size:   *sd.Size,
		}
		eventTime := time.Unix(sd.EventTs, 0).UTC()
		hailEvent.EventTime = eventTime
		return hailEvent, nil

	default:
		return TornadoEvent{}, errors.New("Invalid type")
	}
}

type MsgData struct {
	EmitTs   int64   `json:"EmitTs"`
	FScale   *string `json:"FScale,omitempty"`
	Size     *string `json:"Size,omitempty"`
	Type     string  `json:"StormType"`
	Location string  `json:"Location"`
	County   string  `json:"County"`
	State    string  `json:"State"`
	Lat      float64 `json:"Lat"`
	Lon      float64 `json:"Lon"`
	Comments string  `json:"Comments"`
	Speed    *string `json:"Speed,omitempty"`
	EventTs  int64   `json:"Time"`
}

func (p Process) HandleMessage(msg *kafka.Message) error {
	var stormData MsgData
	// Print the Kafka message metadata and value for debugging
	fmt.Printf("Received message: Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Key), string(msg.Value))
	if err := json.Unmarshal(msg.Value, &stormData); err != nil {
		return errors.New("Unable to parse message: " + err.Error())
	}
	sd, err := determineStormData(stormData)
	if err != nil {
		return errors.New("Unable to determine storm data due to " + err.Error())
	}
	err = sd.Save(p.MRepo.DbRepo)
	if err != nil {
		return err
	}
	return nil
}
