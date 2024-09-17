package storm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testCase struct {
	hourMinute     string
	argDate        time.Time
	errExpected    bool
	errMsg         string
	expectedResult int64
}

func TestStandardizeEventTime(t *testing.T) {
	tests := []testCase{
		{
			hourMinute:     "1233",
			argDate:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			errExpected:    false,
			expectedResult: int64(1704112380),
		},
		{
			hourMinute:     "123344",
			argDate:        time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
			errExpected:    true,
			errMsg:         "Time string must be at least 4 characters long (HHMM)",
			expectedResult: int64(0),
		},
	}
	for _, tc := range tests {
		result, err := standardizeEventTime(tc.hourMinute, tc.argDate)
		assert.Equal(t, result, tc.expectedResult)
		if tc.errExpected {
			assert.EqualError(t, err, tc.errMsg)
		} else {
			assert.Equal(t, err, nil)
		}
	}
}
