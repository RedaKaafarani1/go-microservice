package config

import (
	"path/filepath"
)

// DataDir is the base directory for all data files
// var DataDir = filepath.Join("../..", "data")
var DataDir = filepath.Join(".", "data")

// GetDataFilePath returns the full path for a data file given its name
func GetDataFilePath(filename string) string {
	return filepath.Join(DataDir, filename)
} 