package scenario

import (
	"io/fs"
	"path/filepath"
	"testing"
)

func TestExampleScenarioFilesLoadAndValidate(t *testing.T) {
	root := filepath.Join("..", "..", "test", "scenarios")
	fileCount := 0

	err := filepath.WalkDir(root, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(path) != ".yaml" {
			return nil
		}

		fileCount++
		relativePath, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}

		t.Run(relativePath, func(t *testing.T) {
			s, err := Load(path)
			if err != nil {
				t.Fatalf("Load(%q) returned error: %v", path, err)
			}

			if err := s.Validate(); err != nil {
				t.Fatalf("scenario from %q is invalid: %v", path, err)
			}
		})

		return nil
	})
	if err != nil {
		t.Fatalf("walk scenario files: %v", err)
	}
	if fileCount == 0 {
		t.Fatalf("no YAML scenario files found under %q", root)
	}
}
