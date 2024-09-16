package storm

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

// StormData represents the incoming raw weather data.
type MsgData struct {
	Time     string  `json:"Time"`
	EmitTs   int64   `json:"EmitTs"`
	FScale   *string `json:"FScale,omitempty"`
	Size     *string `json:"Size,omitempty"`
	Location string  `json:"Location"`
	County   string  `json:"County"`
	State    string  `json:"State"`
	Lat      float64 `json:"Lat,string"`
	Lon      float64 `json:"Lon,string"`
	Comments string  `json:"Comments"`
	Speed    *string `json:"Speed,omitempty"`
	EventTs  int64   `json:"EventTs"`
}

const (
	Wind    string = "wind"
	Tornado string = "tornado"
	Hail    string = "hail"
	Invalid string = "invalid"
)

type WeatherData interface {
	GetType() string
}

type WindStorm struct {
	Time     int64   `json:"Time"`
	EmitTs   int64   `json:"EmitTs"`
	Location string  `json:"Location"`
	County   string  `json:"County"`
	State    string  `json:"State"`
	Lat      float64 `json:"Lat"`
	Lon      float64 `json:"Lon"`
	Comments string  `json:"Comments"`
	Speed    string  `json:"Speed"`
	Type     string  `json:"StormType"`
}

func (w WindStorm) GetType() string {
	return Wind
}

type HailStorm struct {
	Time     int64   `json:"Time"`
	Location string  `json:"Location"`
	County   string  `json:"County"`
	State    string  `json:"State"`
	Lat      float64 `json:"Lat"`
	Lon      float64 `json:"Lon"`
	Comments string  `json:"Comments"`
	Size     string  `json:"Size"`
	EmitTs   int64   `json:"EmitTs"`
	Type     string  `json:"StormType"`
}

func (h HailStorm) GetType() string {
	return Hail
}

type TornadoStorm struct {
	Time     int64   `json:"Time"`
	Location string  `json:"Location"`
	County   string  `json:"County"`
	State    string  `json:"State"`
	Lat      float64 `json:"Lat,string"`
	Lon      float64 `json:"Lon,string"`
	Comments string  `json:"Comments"`
	FScale   string  `json:"F_Scale"`
	EmitTs   int64   `json:"EmitTs"`
	Type     string  `json:"StormType"`
}

func (t TornadoStorm) GetType() string {
	return Tornado
}

type InvalidStorm struct {
	Error error
}

func (is InvalidStorm) GetType() string {
	return Invalid
}

func standardizeEventTime(timeStr string, eventDate time.Time) (int64, error) {
	var eventTime int64
	if len(timeStr) < 4 {
		return 0, errors.New("Time string must be at least 4 characters long (HHMM)")
	}
	hour, err := strconv.Atoi((timeStr[:2]))
	if err != nil {
		return eventTime, errors.New("Unable to parse HH in Time field")
	}
	minute, err := strconv.Atoi((timeStr[2:]))
	if err != nil {
		return eventTime, errors.New("Unable to parse MM in Time field")
	}
	eventTime = time.Date(eventDate.Year(), eventDate.Month(), eventDate.Day(), hour, minute, 0, 0, time.UTC).Unix()
	return eventTime, nil
}

func determineStormData(sd MsgData) (WeatherData, error) {
	timeEventTs := time.Unix(sd.EventTs/1000, (sd.EventTs%1000)*int64(time.Millisecond))
	eventTs, err := standardizeEventTime(sd.Time, timeEventTs)

	if err != nil {
		return InvalidStorm{}, err
	}
	if sd.FScale != nil {
		tornadoData := TornadoStorm{
			Comments: sd.Comments,
			Location: sd.Location,
			State:    sd.State,
			Lon:      sd.Lon,
			Lat:      sd.Lat,
			FScale:   *sd.FScale,
			County:   sd.County,
			EmitTs:   sd.EmitTs,
			Time:     eventTs,
		}
		tornadoData.Type = tornadoData.GetType()
		return tornadoData, nil
	} else if sd.Speed != nil {
		windData := WindStorm{
			Comments: sd.Comments,
			Location: sd.Location,
			State:    sd.State,
			County:   sd.County,
			Lat:      sd.Lat,
			Lon:      sd.Lon,
			Speed:    *sd.Speed,
			EmitTs:   sd.EmitTs,
			Time:     eventTs,
		}
		windData.Type = windData.GetType()
		return windData, nil
	} else if sd.Size != nil {
		hailData := HailStorm{
			Comments: sd.Comments,
			Location: sd.Location,
			State:    sd.State,
			County:   sd.County,
			Lat:      sd.Lat,
			Lon:      sd.Lon,
			Size:     *sd.Size,
			Time:     eventTs,
			EmitTs:   sd.EmitTs,
		}
		hailData.Type = hailData.GetType()
		return hailData, nil
	} else {
		return InvalidStorm{}, nil
	}
}

func MarshalJson(sd WeatherData) ([]byte, error) {
	switch storm := sd.(type) {
	case TornadoStorm:
		jsonData, err := json.Marshal(storm)
		if err != nil {
			return []byte{}, errors.New("Unable to marshal tornado data")
		}
		return jsonData, nil
	case HailStorm:
		jsonData, err := json.Marshal(storm)
		if err != nil {
			return []byte{}, errors.New("Unable to marshal tornado data")
		}
		return jsonData, nil
	case WindStorm:
		jsonData, err := json.Marshal(storm)
		if err != nil {
			return []byte{}, errors.New("Unable to marshal tornado data")
		}
		return jsonData, nil
	case InvalidStorm:
		return []byte{}, errors.New("Invalid message type")
	default:
		return []byte{}, errors.New("Invalid message type")
	}
}

func handleMessage(kp *kafka.Producer, msg *kafka.Message, topic string) error {
	var stormData MsgData
	// Print the Kafka message metadata and value for debugging
	log.Printf(fmt.Sprintf("Received message: Topic: %s, Partition: %d, Offset: %d, Value: %s\n",
		*msg.TopicPartition.Topic, msg.TopicPartition.Partition, msg.TopicPartition.Offset, string(msg.Value)))
	if err := json.Unmarshal(msg.Value, &stormData); err != nil {
		return errors.New("Unable to parse message: " + err.Error())
	}
	sd, err := determineStormData(stormData)
	if err != nil {
		return errors.New("Unable to determine storm data due to " + err.Error())
	}
	jsonData, err := MarshalJson(sd)
	if err != nil {
		return err
	}
	kp.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: jsonData,
	}, nil)

	log.Println("Processed message and push to " + topic)
	return nil
}
