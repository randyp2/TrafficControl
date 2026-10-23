package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/randyp2/trafficcontrol/internal/engine"
	"github.com/randyp2/trafficcontrol/internal/scenario"
)

// runCommand loads a scenario and enables scheduler diagnostics with --verbose.
func runCommand(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	verbose := flags.Bool("verbose", false, "show scheduler diagnostics on stderr")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return errors.New("usage: trafficcontrol run [--verbose] <scenario.yaml>")
	}

	scenarioPath := flags.Arg(0)
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

	// Clean signal listener
	defer stop()

	var debugOutput io.Writer = io.Discard
	if *verbose {
		debugOutput = os.Stderr
	}
	debugLogger := log.New(debugOutput, "", 0)

	consoleReporter := newConsoleReporter(os.Stdout)
	return engine.Run(ctx, s, consoleReporter, debugLogger)
}
