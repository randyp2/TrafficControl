package scenario

import "testing"

func TestEventValidateActionsWithoutRates(t *testing.T) {
	actions := []Action{
		ActionPause,
		ActionResume,
		ActionStop,
	}

	for _, action := range actions {
		t.Run(string(action), func(t *testing.T) {
			event := Event{
				Stream: "telemetry",
				Action: action,
				Rate:   1,
			}

			if err := event.Validate(); err == nil {
				t.Fatalf("Event.Validate() accepted a rate for action %q", action)
			}
		})
	}
}

func TestEventValidateSetRate(t *testing.T) {
	tests := []struct {
		name    string
		rate    int
		wantErr bool
	}{
		{
			name: "positive rate",
			rate: 10,
		},
		{
			name:    "zero rate",
			rate:    0,
			wantErr: true,
		},
		{
			name:    "negative rate",
			rate:    -1,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			event := Event{
				Stream: "telemetry",
				Action: ActionSetRate,
				Rate:   tt.rate,
			}

			err := event.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Event.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestScenarioValidateEventStreamReference(t *testing.T) {
	tests := []struct {
		name        string
		eventStream string
		wantErr     bool
	}{
		{
			name:        "existing stream",
			eventStream: "telemetry",
		},
		{
			name:        "missing stream",
			eventStream: "backend",
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := Scenario{
				Name: "stream-reference",
				Streams: []Stream{
					{
						Name:     "telemetry",
						Protocol: ProtocolUDP,
						Target:   "127.0.0.1:5000",
						Rate:     1,
					},
				},
				Events: []Event{
					{
						Stream: tt.eventStream,
						Action: ActionStop,
					},
				},
			}

			err := s.Validate()
			if (err != nil) != tt.wantErr {
				t.Fatalf("Scenario.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
