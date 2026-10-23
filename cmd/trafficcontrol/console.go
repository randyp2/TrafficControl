package main

import (
	"io"
	"os"
)

const (
	colorGreen  = "\x1b[32m"
	colorYellow = "\x1b[33m"
	colorRed    = "\x1b[31m"
	colorCyan   = "\x1b[36m"
)

// consoleColor disables ANSI output for redirected streams and opt-out environments.
func consoleColor(output io.Writer) bool {
	if os.Getenv("NO_COLOR") != "" || os.Getenv("TERM") == "dumb" {
		return false
	}
	file, ok := output.(*os.File)
	if !ok {
		return false
	}
	info, err := file.Stat()
	return err == nil && info.Mode()&os.ModeCharDevice != 0
}

func colorize(text, color string, enabled bool) string {
	if !enabled {
		return text
	}
	return color + text + "\x1b[0m"
}
