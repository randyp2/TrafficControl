package main

import (
	"errors"
	"fmt"
)

// Run runs the respective function based on the first command line
// argument.
func run(args []string) error {
	if len(args) < 1 {
		return errors.New("usage: trafficcontrol <command>")
	}

	switch args[0] {
	case "run":
		return runCommand(args[1:])
	default:
		return fmt.Errorf(
			"trafficcontrol: '%s' is not a trafficcontrol command. See 'trafficcontrol --help'",
			args[0],
		)
	}
}
