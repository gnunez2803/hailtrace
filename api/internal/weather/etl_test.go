package weather

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testStormCase struct {
	msg            MsgData
	expectedResult WeatherDbEvent
	err            error
}

func TestDetermineStorm(t *testing.T) {
	size := "100"
	fScale := "UNK"
	speed := "20"
	testCases := []testStormCase{
		{
			msg: MsgData{
				Type:  "wind",
				Speed: &speed,
			},
			err:            nil,
			expectedResult: WindEvent{},
		},
		{
			msg: MsgData{
				FScale: &fScale,
				Type:   "tornado",
			},
			err:            nil,
			expectedResult: TornadoEvent{},
		},
		{
			msg: MsgData{
				Size: &size,
				Type: "hail",
			},
			err:            nil,
			expectedResult: HailEvent{},
		},
	}
	for _, tc := range testCases {
		event, err := determineStormData(tc.msg)
		assert.Equal(t, err, tc.err)
		assert.Equal(t, reflect.TypeOf(event), reflect.TypeOf(tc.expectedResult))
	}
}
