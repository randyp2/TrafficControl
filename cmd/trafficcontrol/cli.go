package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
)

// version can be set for releases with -ldflags '-X main.version=v1.0.0'.
var version = "dev"

const rootHelp = `Usage: trafficcontrol <command> [options]

Generate UDP or TCP traffic from YAML scenarios and measure received traffic.

Commands:
  run       Execute a scenario
  listen    Receive traffic and report metrics
  help      Show help for a command

Options:
  -h, --help    Show this help
  --version     Show the build version

Examples:
  trafficcontrol run examples/scenarios/basic/udp.yaml
  trafficcontrol listen --protocol udp --address 127.0.0.1:5000
  trafficcontrol help run`

// Run runs the respective function based on the first command line
// argument.
func run(args []string) error {
	if len(args) < 1 {
		return errors.New(rootHelp)
	}

	switch args[0] {
	case "-h", "--help":
		if len(args) != 1 {
			return errors.New("usage: trafficcontrol help [command]")
		}
		fmt.Fprintln(os.Stdout, rootHelp)
		return flag.ErrHelp
	case "help":
		if len(args) == 1 {
			return run([]string{"--help"})
		}
		if len(args) != 2 {
			return errors.New("usage: trafficcontrol help [command]")
		}
		switch args[1] {
		case "run", "listen":
			return run([]string{args[1], "--help"})
		default:
			return fmt.Errorf("unknown help topic %q. See 'trafficcontrol --help'", args[1])
		}
	case "--version":
		if len(args) != 1 {
			return errors.New("usage: trafficcontrol --version")
		}
		fmt.Fprintln(os.Stdout, "trafficcontrol", version)
		return nil
	case "run":
		return runCommand(args[1:])
	case "listen":
		return listenCommand(args[1:])
	default:
		return fmt.Errorf(
			"trafficcontrol: '%s' is not a trafficcontrol command. See 'trafficcontrol --help'",
			args[0],
		)
	}
}

// parseCommandFlags prints explicit help; main owns printing all command errors.
func parseCommandFlags(flags *flag.FlagSet, args []string, help string) error {
	flags.SetOutput(io.Discard)
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fmt.Fprintln(os.Stdout, help)
			flags.SetOutput(os.Stdout)
			flags.PrintDefaults()
			return err
		}
		return fmt.Errorf("%w\nSee 'trafficcontrol %s --help' for usage.", err, flags.Name())
	}
	return nil
}
