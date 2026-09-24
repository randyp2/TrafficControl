package scenario

import (
	"time"
)

type Action string

const (
	ActionSetRate Action = "set_rate"
	ActionStop    Action = "stop"

	ActionPause  Action = "pause"
	ActionResume Action = "resume"
)

type Event struct {
	At     time.Duration `yaml:"at"`
	Stream string        `yaml:"stream"`
	Action Action        `yaml:"action"`

	Rate int `yaml:"rate,omitempty"`
}
