package scenario

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestEventValidateJitter(t *testing.T) {
	for _, tt := range []struct {
		name    string
		event   Event
		wantErr bool
	}{
		{name: "disable", event: Event{Action: ActionSetJitter}},
		{name: "positive", event: Event{Action: ActionSetJitter, Jitter: time.Millisecond}},
		{name: "negative", event: Event{Action: ActionSetJitter, Jitter: -time.Millisecond}, wantErr: true},
		{name: "reject rate", event: Event{Action: ActionSetJitter, Rate: 1}, wantErr: true},
		{name: "reject latency", event: Event{Action: ActionSetJitter, Latency: time.Millisecond}, wantErr: true},
		{name: "reject loss", event: Event{Action: ActionSetJitter, LossPercent: 1}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			event := tt.event
			event.Stream = "telemetry"
			if err := event.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate error = %v, wantErr %t", err, tt.wantErr)
			}
		})
	}
	for _, action := range []Action{ActionSetRate, ActionSetLatency, ActionSetLoss, ActionPause, ActionResume, ActionStop} {
		t.Run(string(action)+" rejects jitter", func(t *testing.T) {
			event := Event{Stream: "telemetry", Action: action, Rate: 1, Jitter: time.Millisecond}
			if action != ActionSetRate {
				event.Rate = 0
			}
			if err := event.Validate(); err == nil {
				t.Fatalf("action %q accepted jitter", action)
			}
		})
	}
}

func TestScenarioValidateJitterTimeline(t *testing.T) {
	latency := Event{Stream: "telemetry", Action: ActionSetLatency, Latency: 100 * time.Millisecond}
	jitter := Event{At: time.Second, Stream: "telemetry", Action: ActionSetJitter, Jitter: 10 * time.Millisecond}
	off := Event{At: 2 * time.Second, Stream: "telemetry", Action: ActionSetJitter}
	lower := Event{At: 3 * time.Second, Stream: "telemetry", Action: ActionSetLatency, Latency: 5 * time.Millisecond}
	other := jitter
	other.Stream = "other"
	equal := jitter
	equal.Jitter = latency.Latency
	sameTime := jitter
	sameTime.At = 0
	maxLatency := latency
	maxLatency.Latency = time.Duration(1<<63 - 1)
	lastSafeLatency := maxLatency
	lastSafeLatency.Latency--
	oneNanosecond := jitter
	oneNanosecond.Jitter = time.Nanosecond
	for _, tt := range []struct {
		name    string
		events  []Event
		wantErr bool
	}{
		{name: "latency before jitter", events: []Event{latency, jitter}},
		{name: "jitter equal to latency", events: []Event{latency, equal}},
		{name: "disable before lowering latency", events: []Event{latency, jitter, off, lower}},
		{name: "unsorted input", events: []Event{jitter, latency}},
		{name: "same-time events preserve order", events: []Event{latency, sameTime}},
		{name: "jitter without latency", events: []Event{jitter}, wantErr: true},
		{name: "jitter before same-time latency", events: []Event{sameTime, latency}, wantErr: true},
		{name: "lower latency below jitter", events: []Event{latency, jitter, lower}, wantErr: true},
		{name: "stream settings are independent", events: []Event{latency, other}, wantErr: true},
		{name: "maximum latency without jitter", events: []Event{maxLatency}},
		{name: "maximum combined delay", events: []Event{lastSafeLatency, oneNanosecond}},
		{name: "jitter causes overflow", events: []Event{maxLatency, oneNanosecond}, wantErr: true},
		{name: "latency change causes overflow", events: []Event{latency, oneNanosecond, {
			At: 2 * time.Second, Stream: "telemetry", Action: ActionSetLatency, Latency: maxLatency.Latency,
		}}, wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := Scenario{
				Name: "jitter-validation",
				Streams: []Stream{
					{Name: "telemetry", Protocol: ProtocolUDP, Target: "127.0.0.1:5000", Rate: 1},
					{Name: "other", Protocol: ProtocolUDP, Target: "127.0.0.1:5000", Rate: 1},
				},
				Events: tt.events,
			}
			original := append([]Event(nil), s.Events...)
			if err := s.Validate(); (err != nil) != tt.wantErr {
				t.Fatalf("Validate error = %v, wantErr %t", err, tt.wantErr)
			}
			if !slices.Equal(s.Events, original) {
				t.Fatal("validation changed the original event order")
			}
		})
	}
}

func TestLoadJitterDuration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jitter.yaml")
	if err := os.WriteFile(path, []byte(`name: jitter-config
streams:
  - name: telemetry
    protocol: udp
    target: 127.0.0.1:5000
    rate: 1
events:
  - at: 0s
    stream: telemetry
    action: set_latency
    latency: 100ms
  - at: 1s
    stream: telemetry
    action: set_jitter
    jitter: 10ms
`), 0600); err != nil {
		t.Fatal(err)
	}
	s, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := s.Validate(); err != nil {
		t.Fatal(err)
	}
	if got := s.Events[1].Jitter; got != 10*time.Millisecond {
		t.Fatalf("jitter = %s, want 10ms", got)
	}
}
