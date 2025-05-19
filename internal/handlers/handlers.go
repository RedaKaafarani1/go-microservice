package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"time"

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
	startTime := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.SearchRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Validate request
	if req.NAFCode == "" {
		http.Error(w, "NAF code is required", http.StatusBadRequest)
		return
	}

	// Get geometry based on request type
	var geometry interface{}
	if req.Type == "FeatureCollection" {
		if len(req.Features) == 0 {
			http.Error(w, "GeoJSON feature is required", http.StatusBadRequest)
			return
		}
		if req.Features[0].Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		geometry = req.Geometry
	} else {
		http.Error(w, "Invalid GeoJSON type. Must be either 'Feature' or 'FeatureCollection'", http.StatusBadRequest)
		return
	}

	// Search for businesses
	geojsonStr, _ := json.Marshal(geometry)
	businesses, err := h.csvService.SearchBusinesses(string(geojsonStr), req.NAFCode)
	if err != nil {
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	// Return results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(businesses); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	// Log processing time
	duration := time.Since(startTime)
	log.Printf("Request processed in %v\n", duration)
}

// IrisHandler handles IRIS data requests
type IrisHandler struct {
	csvService *services.CSVService
}

// NewIrisHandler creates a new IrisHandler instance
func NewIrisHandler(csvService *services.CSVService) *IrisHandler {
	return &IrisHandler{
		csvService: csvService,
	}
}

// HandleIrisData handles the IRIS data request
func (h *IrisHandler) HandleIrisData(w http.ResponseWriter, r *http.Request) {
	startTime := time.Now()

	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req models.IrisRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid request format", http.StatusBadRequest)
		return
	}

	// Get geometry based on request type
	var geometry interface{}
	if req.Type == "FeatureCollection" {
		if len(req.Features) == 0 {
			http.Error(w, "GeoJSON feature is required", http.StatusBadRequest)
			return
		}
		if req.Features[0].Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		geometry = req.Geometry
	} else {
		http.Error(w, "Invalid GeoJSON type. Must be either 'Feature' or 'FeatureCollection'", http.StatusBadRequest)
		return
	}

	// Get IRIS data
	geojsonStr, _ := json.Marshal(geometry)
	irisData, err := h.csvService.GetIrisData(string(geojsonStr))
	if err != nil {
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	// Return results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(irisData); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	// Log processing time
	duration := time.Since(startTime)
	log.Printf("Request processed in %v\n", duration)
} 