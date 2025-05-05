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
	// Get the absolute path to the CSV file
	csvPath := filepath.Join("./", "data","StockEtablissement_open_only_and_geo_and_names.csv")
	absPath, err := filepath.Abs(csvPath)
	if err != nil {
		log.Fatalf("Error getting absolute path: %v", err)
	}

	// Check if the CSV file exists
	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		log.Fatalf("CSV file not found at: %s", absPath)
	}

	log.Printf("Using CSV file at: %s", absPath)

	// Initialize services and handlers
	csvService := services.NewCSVService(absPath)
	searchHandler := handlers.NewSearchHandler(csvService)

	// Set up routes
	http.HandleFunc("/search", searchHandler.HandleSearch)

	// Start server
	port := "8080"
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
} 