package handlers

import (
	"encoding/json"
	"log"
	"net/http"

	"csv-processor/internal/models"
	"csv-processor/internal/services"
)

// SearchHandler handles search requests
type SearchHandler struct {
	csvService *services.CSVService
}

// NewSearchHandler creates a new SearchHandler instance
func NewSearchHandler(csvService *services.CSVService) *SearchHandler {
	return &SearchHandler{
		csvService: csvService,
	}
}

// HandleSearch handles the search request
func (h *SearchHandler) HandleSearch(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		log.Printf("Error decoding request: %v\n", err)
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.NAFCode == "" {
		http.Error(w, "NAF code is required", http.StatusBadRequest)
		return
	}

	if len(req.Features) == 0 {
		http.Error(w, "GeoJSON feature is required", http.StatusBadRequest)
		return
	}

	if req.Features[0].Geometry.Type != "Polygon" {
		http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
		return
	}

	// Log the request details
	log.Printf("Processing request for NAF code: %s with polygon of %d points\n",
		req.NAFCode, len(req.Features[0].Geometry.Coordinates[0]))

	// Search for businesses
	geojsonStr, _ := json.Marshal(req.Features[0].Geometry)
	businesses, err := h.csvService.SearchBusinesses(string(geojsonStr), req.NAFCode)
	if err != nil {
		log.Printf("Error searching businesses: %v\n", err)
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	// Return results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(businesses); err != nil {
		log.Printf("Error encoding response: %v\n", err)
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}
} 