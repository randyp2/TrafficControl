package main

import (
	"errors"
	"fmt"
)

func runCommand(args []string) error {
	if len(args) != 1 {
		return errors.New("usage: trafficcontrol run <command>")
	}

	commandPath := args[0]

	fmt.Printf("running command %s\n", commandPath)

	return nil

}
