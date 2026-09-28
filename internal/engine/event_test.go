package engine

import (
	"testing"
	"time"

	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func TestHandleEvent(t *testing.T) {
	tests := []struct {
		name         string
		initialState streamState
		event        scenario.Event
		wantState    streamState
		wantStop     bool
		wantErr      bool
	}{
		{
			name: "pause running stream",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.ActionPause,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamPaused,
			},
		},
		{
			name: "pause already paused stream",
			initialState: streamState{
				targetRate: 10,
				status:     streamPaused,
			},
			event: scenario.Event{
				Action: scenario.ActionPause,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamPaused,
			},
		},
		{
			name: "resume paused stream",
			initialState: streamState{
				targetRate: 10,
				status:     streamPaused,
			},
			event: scenario.Event{
				Action: scenario.ActionResume,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
		},
		{
			name: "resume already running stream",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.ActionResume,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
		},
		{
			name: "set rate while running",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.ActionSetRate,
				Rate:   25,
			},
			wantState: streamState{
				targetRate: 25,
				status:     streamRunning,
			},
		},
		{
			name: "set rate while paused",
			initialState: streamState{
				targetRate: 10,
				status:     streamPaused,
			},
			event: scenario.Event{
				Action: scenario.ActionSetRate,
				Rate:   25,
			},
			wantState: streamState{
				targetRate: 25,
				status:     streamPaused,
			},
		},
		{
			name: "stop stream",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.ActionStop,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			wantStop: true,
		},
		{
			name: "reject invalid set rate",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.ActionSetRate,
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			wantErr: true,
		},
		{
			name: "reject unknown action",
			initialState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			event: scenario.Event{
				Action: scenario.Action("unknown"),
			},
			wantState: streamState{
				targetRate: 10,
				status:     streamRunning,
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ticker := time.NewTicker(time.Hour)
			defer ticker.Stop()

			state := tt.initialState
			gotStop, err := handleEvent(ticker, tt.event, &state)

			if (err != nil) != tt.wantErr {
				t.Fatalf("handleEvent() error = %v, wantErr %v", err, tt.wantErr)
			}
			if gotStop != tt.wantStop {
				t.Fatalf("handleEvent() stop = %v, want %v", gotStop, tt.wantStop)
			}
			if state != tt.wantState {
				t.Fatalf("handleEvent() state = %#v, want %#v", state, tt.wantState)
			}
		})
	}
}
