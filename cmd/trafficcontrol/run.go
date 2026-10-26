package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/signal"

	"github.com/randyp2/trafficcontrol/internal/engine"
	"github.com/randyp2/trafficcontrol/internal/scenario"
)

const runUsage = "trafficcontrol run [--verbose] <scenario.yaml>"

const runHelp = `Usage: ` + runUsage + `

Load and validate a YAML scenario, then generate its scheduled traffic.
Flags must appear before the scenario path. Use -- before a path starting with -.
Stop with Ctrl-C to print a final summary.

Examples:
  trafficcontrol run examples/scenarios/basic/udp.yaml
  trafficcontrol run --verbose examples/scenarios/loss/toggle.yaml

Options:`

// runCommand loads a scenario and enables scheduler diagnostics with --verbose.
func runCommand(args []string) error {
	flags := flag.NewFlagSet("run", flag.ContinueOnError)
	verbose := flags.Bool("verbose", false, "show scheduler diagnostics on stderr")
	if err := parseCommandFlags(flags, args, runHelp); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("usage: %s\nflags must appear before the scenario path. See 'trafficcontrol run --help'", runUsage)
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
