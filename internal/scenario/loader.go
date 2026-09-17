package scenario

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// Load takes a file path, reads the file, and converts the bytes into
// our Scenario model
func Load(path string) (Scenario, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Scenario{}, fmt.Errorf(
			"error reading scenario config file: %w",
			err,
		)
	}

	var scenario Scenario
	if err := yaml.Unmarshal(data, &scenario); err != nil {
		return Scenario{}, fmt.Errorf("error parsing yaml config: %w", err)
	}

	return scenario, nil
}
