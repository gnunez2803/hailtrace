package storm

import "testing"

func TestMarshalJson(t *testing.T) {
	tests := []struct {
		name    string
		input   WeatherData
		want    string
		wantErr bool
	}{
		{
			name: "TornadoStorm",
			input: TornadoStorm{
				FScale: "100",
			},
			want:    `{"id":"T1","wind":150}`,
			wantErr: false,
		},
		{
			name:    "HailStorm",
			input:   HailStorm{ID: "H1", Size: 5},
			want:    `{"id":"H1","size":5}`,
			wantErr: false,
		},
		{
			name:    "WindStorm",
			input:   WindStorm{ID: "W1", Speed: 60},
			want:    `{"id":"W1","speed":60}`,
			wantErr: false,
		},
		{
			name:    "InvalidStorm",
			input:   InvalidStorm{},
			want:    "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MarshalJson(tt.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("MarshalJson() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if string(got) != tt.want {
				t.Errorf("MarshalJson() = %v, want %v", string(got), tt.want)
			}
		})
	}
}
