package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
)

// main runs the CLI and reports command errors.
func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
