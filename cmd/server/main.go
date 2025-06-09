package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"csv-processor/internal/config"
	"csv-processor/internal/handlers"
	"csv-processor/internal/services"
)

const (
	AppVersion = "1.0.0"
)

func main() {
	log.Printf("Starting CSV Processor v%s", AppVersion)
	
	// Get the absolute path to the CSV files
	businessCSVPath := config.GetDataFilePath("StockEtablissement_open_only_and_geo_and_names.csv")
	irisCSVPath := config.GetDataFilePath("iris-data-with-polygon-coord-standard-with-area-and-calculations.csv")
	communeCSVPath := config.GetDataFilePath("full_commune_from_iris-05092024.csv")
	qpCSVPath := config.GetDataFilePath("final_special_zones-06092024.csv")
	incomeCSVPath := config.GetDataFilePath("chiffres-cles-2024.csv")

	businessAbsPath, err := filepath.Abs(businessCSVPath)
	if err != nil {
		log.Fatalf("Error getting absolute path for business CSV: %v", err)
	}

	irisAbsPath, err := filepath.Abs(irisCSVPath)
	if err != nil {
		log.Fatalf("Error getting absolute path for IRIS CSV: %v", err)
	}

	qpAbsPath, err := filepath.Abs(qpCSVPath)
	if err != nil {
		log.Fatalf("Error getting absolute path for QP CSV: %v", err)
	}

	communeAbsPath, err := filepath.Abs(communeCSVPath)
	if err != nil {
		log.Fatalf("Error getting absolute path for commune CSV: %v", err)
	}

	incomeAbsPath, err := filepath.Abs(incomeCSVPath)
	if err != nil {
		log.Fatalf("Error getting absolute path for income CSV: %v", err)
	}

	// Check if the CSV files exist
	if _, err := os.Stat(businessAbsPath); os.IsNotExist(err) {
		log.Fatalf("Business CSV file not found at: %s", businessAbsPath)
	}
	if _, err := os.Stat(irisAbsPath); os.IsNotExist(err) {
		log.Fatalf("IRIS CSV file not found at: %s", irisAbsPath)
	}

	if _, err := os.Stat(qpAbsPath); os.IsNotExist(err) {
		log.Fatalf("QP CSV file not found at: %s", qpAbsPath)
	}

	if _, err := os.Stat(communeAbsPath); os.IsNotExist(err) {
		log.Fatalf("Commune CSV file not found at: %s", communeAbsPath)
	}

	if _, err := os.Stat(incomeAbsPath); os.IsNotExist(err) {
		log.Fatalf("Income CSV file not found at: %s", incomeAbsPath)
	}

	log.Printf("Using business CSV file at: %s", businessAbsPath)
	log.Printf("Using IRIS CSV file at: %s", irisAbsPath)
	log.Printf("Using QP CSV file at: %s", qpAbsPath)
	log.Printf("Using commune CSV file at: %s", communeAbsPath)
	log.Printf("Using income CSV file at: %s", incomeAbsPath)
	// Initialize services and handlers
	csvService := services.NewCSVService(businessAbsPath, irisAbsPath, qpAbsPath, communeAbsPath, incomeAbsPath)
	searchHandler := handlers.NewSearchHandler(csvService)
	irisHandler := handlers.NewIrisHandler(csvService)
	incomeHandler := handlers.NewIncomeHandler(csvService)

	// Set up routes
	http.HandleFunc("/competitor-search", searchHandler.HandleSearch)
	http.HandleFunc("/competitor-count", searchHandler.HandleCompetitorCount)
	http.HandleFunc("/iris-data", irisHandler.HandleIrisData)
	http.HandleFunc("/competition", incomeHandler.HandleCompetitionData)

	// Start server
	port := "8080"
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}