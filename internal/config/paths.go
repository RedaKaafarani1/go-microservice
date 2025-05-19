package config

import (
	"os"
	"path/filepath"
)

// DataDir is the base directory for all data files
var DataDir string

func init() {
	// Get data directory from environment variable, default to "./data" if not set
	if envDataDir := os.Getenv("DATA_DIR"); envDataDir != "" {
		DataDir = envDataDir
	} else {
		DataDir = filepath.Join(".", "data")
	}
}

// GetDataFilePath returns the full path for a data file given its name
func GetDataFilePath(filename string) string {
	return filepath.Join(DataDir, filename)
} 