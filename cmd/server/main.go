package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"

	"csv-processor/internal/handlers"
	"csv-processor/internal/services"
)

func main() {
	// Get the absolute path to the CSV files
	businessCSVPath := filepath.Join("./", "data", "StockEtablissement_open_only_and_geo_and_names.csv")
	irisCSVPath := filepath.Join("./", "data", "iris-data-with-polygon-coord-standard-with-area-and-calculations.csv")
	qpCSVPath := filepath.Join("./", "data", "final_special_zones-06092024.csv")

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

	log.Printf("Using business CSV file at: %s", businessAbsPath)
	log.Printf("Using IRIS CSV file at: %s", irisAbsPath)
	log.Printf("Using QP CSV file at: %s", qpAbsPath)
	// Initialize services and handlers
	csvService := services.NewCSVService(businessAbsPath, irisAbsPath, qpAbsPath)
	searchHandler := handlers.NewSearchHandler(csvService)
	irisHandler := handlers.NewIrisHandler(csvService)

	// Set up routes
	http.HandleFunc("/search", searchHandler.HandleSearch)
	http.HandleFunc("/iris", irisHandler.HandleIrisData)

	// Start server
	port := "8080"
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
} 