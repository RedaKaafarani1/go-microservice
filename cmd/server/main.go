package main

import (
	"log"
	"net/http"
	"os"

	"csv-processor/internal/config"
	"csv-processor/internal/handlers"
	"csv-processor/internal/services"
)

const (
	AppVersion = "1.0.0"
)

func main() {
	log.Printf("Starting CSV Processor v%s", AppVersion)
	
	// Get CSV config
	csvConfig := config.GetCSVConfig()

	// Check if the CSV files exist
	if _, err := os.Stat(config.GetDataFilePath(csvConfig.BusinessData)); os.IsNotExist(err) {
		log.Fatalf("Business CSV file not found at: %s", config.GetDataFilePath(csvConfig.BusinessData))
	}
	if _, err := os.Stat(config.GetDataFilePath(csvConfig.IrisData)); os.IsNotExist(err) {
		log.Fatalf("IRIS CSV file not found at: %s", config.GetDataFilePath(csvConfig.IrisData))
	}
	if _, err := os.Stat(config.GetDataFilePath(csvConfig.QPData)); os.IsNotExist(err) {
		log.Fatalf("QP CSV file not found at: %s", config.GetDataFilePath(csvConfig.QPData))
	}
	if _, err := os.Stat(config.GetDataFilePath(csvConfig.CommuneData)); os.IsNotExist(err) {
		log.Fatalf("Commune CSV file not found at: %s", config.GetDataFilePath(csvConfig.CommuneData))
	}
	if _, err := os.Stat(config.GetDataFilePath(csvConfig.CompetitionData)); os.IsNotExist(err) {
		log.Fatalf("Competition CSV file not found at: %s", config.GetDataFilePath(csvConfig.CompetitionData))
	}

	log.Printf("Using business CSV file at: %s", config.GetDataFilePath(csvConfig.BusinessData))
	log.Printf("Using IRIS CSV file at: %s", config.GetDataFilePath(csvConfig.IrisData))
	log.Printf("Using QP CSV file at: %s", config.GetDataFilePath(csvConfig.QPData))
	log.Printf("Using commune CSV file at: %s", config.GetDataFilePath(csvConfig.CommuneData))
	log.Printf("Using competition CSV file at: %s", config.GetDataFilePath(csvConfig.CompetitionData))

	// Initialize services and handlers
	csvService := services.NewCSVService()
	searchHandler := handlers.NewSearchHandler(csvService)
	irisHandler := handlers.NewIrisHandler(csvService)

	// Set up routes
	http.HandleFunc("/competitor-search", searchHandler.HandleSearch)
	http.HandleFunc("/competitor-count", searchHandler.HandleCompetitorCount)
	http.HandleFunc("/competition-data", searchHandler.HandleCompetitionData)
	http.HandleFunc("/iris-data", irisHandler.HandleIrisData)

	// Start server
	port := "8080"
	log.Printf("Server starting on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Error starting server: %v", err)
	}
}