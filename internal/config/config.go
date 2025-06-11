package config

import (
	"encoding/json"
	"os"
	"path/filepath"
)

// DataDir is the base directory for all data files
var DataDir string

type CSVConfig struct {
	BusinessData     string `json:"business_data"`
	CompetitionData  string `json:"competition_data"`
	CommuneCrimes    string `json:"commune_crimes"`
	DepartmentCrimes string `json:"department_crimes"`
	IrisData         string `json:"iris_data"`
	CommuneData      string `json:"commune_data"`
	QPData           string `json:"qp_data"`
}

var csvConfig CSVConfig

func init() {
	// Set up data directory
	if envDataDir := os.Getenv("DATA_DIR"); envDataDir != "" {
		DataDir = envDataDir
	} else {
		DataDir = filepath.Join(".", "data")
	}

	// Default paths
	csvConfig = CSVConfig{
		BusinessData:     "StockEtablissement_open_only_and_geo_and_names.csv",
		CompetitionData:  "chiffres-cles-2024.csv",
		CommuneCrimes:    "crimes_per_commune.csv",
		DepartmentCrimes: "dep-indexed-crime-data.csv",
		IrisData:         "iris-data-with-polygon-coord-standard-with-area-and-calculations.csv",
		CommuneData:      "full_commune_from_iris-05092024.csv",
		QPData:           "final_special_zones-06092024.csv",
	}

	// Try to load config from file
	if configFile, err := os.Open("config.json"); err == nil {
		defer configFile.Close()
		json.NewDecoder(configFile).Decode(&csvConfig)
	}
}

// GetDataFilePath returns the absolute path for a data file
func GetDataFilePath(filename string) string {
	return filepath.Join(DataDir, filename)
}

// GetCSVConfig returns the CSV configuration
func GetCSVConfig() CSVConfig {
	return csvConfig
} 