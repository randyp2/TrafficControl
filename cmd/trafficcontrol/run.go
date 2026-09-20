package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"

	"github.com/randyp2/trafficcontrol/internal/engine"
	"github.com/randyp2/trafficcontrol/internal/scenario"
)

func runCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: trafficcontrol run <command>")
	}

	scenarioPath := args[0]
	s, err := scenario.Load(scenarioPath)
	if err != nil {
		return err
	}

	if err := s.Validate(); err != nil {
		return fmt.Errorf("invalid scenario: %w", err)
	}

	ctx, stop := signal.NotifyContext(
		context.Background(), // Parent root context
		os.Interrupt,
	)

	defer stop() // Clean signal listener

	return engine.RunStream(ctx, s.Streams[0])
}
