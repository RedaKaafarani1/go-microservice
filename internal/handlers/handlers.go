package handlers

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	// "os"
	"time"

	"csv-processor/internal/models"
	"csv-processor/internal/services"
)

// Point represents a 2D point
type Point struct {
	X float64
	Y float64
}

// Constants for polygon complexity thresholds
const (
	// Maximum number of points before simplification is considered
	maxPoints = 700
	// Minimum number of points to consider for simplification
	minPoints = 400
	// Base percentage of bounding box diagonal for epsilon
	baseEpsilonPercent = 0.1 // 0.1% of the diagonal
)

// calculatePolygonArea calculates the area of a polygon using the shoelace formula
func calculatePolygonArea(points []Point) float64 {
	area := 0.0
	j := len(points) - 1
	for i := 0; i < len(points); i++ {
		area += (points[j].X + points[i].X) * (points[j].Y - points[i].Y)
		j = i
	}
	return math.Abs(area) / 2
}

// calculateBoundingBoxDiagonal calculates the diagonal length of the polygon's bounding box
func calculateBoundingBoxDiagonal(points []Point) float64 {
	if len(points) == 0 {
		return 0
	}

	minX, minY := points[0].X, points[0].Y
	maxX, maxY := points[0].X, points[0].Y

	for _, p := range points {
		if p.X < minX {
			minX = p.X
		}
		if p.X > maxX {
			maxX = p.X
		}
		if p.Y < minY {
			minY = p.Y
		}
		if p.Y > maxY {
			maxY = p.Y
		}
	}

	// Calculate diagonal length
	dx := maxX - minX
	dy := maxY - minY
	return math.Sqrt(dx*dx + dy*dy)
}

// calculatePolygonComplexity determines if a polygon needs simplification and returns an appropriate epsilon value
func calculatePolygonComplexity(points []Point) (bool, float64) {
	numPoints := len(points)
	
	// If the polygon has fewer points than the minimum threshold, no simplification needed
	if numPoints < minPoints {
		return false, 0
	}

	// Calculate polygon area
	area := calculatePolygonArea(points)
	
	// Calculate points per unit area
	pointsPerArea := float64(numPoints) / area
	
	// Determine if simplification is needed
	needsSimplification := numPoints > maxPoints || pointsPerArea > 700

	if !needsSimplification {
		return false, 0
	}

	// Calculate bounding box diagonal
	diagonal := calculateBoundingBoxDiagonal(points)
	
	// Calculate base epsilon as a percentage of the diagonal
	baseEpsilon := diagonal * baseEpsilonPercent / 100.0

	// Scale epsilon based on polygon complexity
	// More points = larger epsilon (more simplification)
	epsilon := baseEpsilon * math.Pow(float64(numPoints)/float64(minPoints), 0.55)
	
	// Cap the epsilon to prevent over-simplification
	// Maximum epsilon is 1% of the diagonal
	maxEpsilon := diagonal * 0.01
	if epsilon > maxEpsilon {
		epsilon = maxEpsilon
	}

	log.Printf("Polygon diagonal: %f, Base epsilon: %f, Final epsilon: %f", diagonal, baseEpsilon, epsilon)
	return true, epsilon
}

// perpendicularDistance calculates the perpendicular distance from a point to a line segment
func perpendicularDistance(point Point, lineStart Point, lineEnd Point) float64 {
	// If the line segment is actually a point, return distance to that point
	if lineStart.X == lineEnd.X && lineStart.Y == lineEnd.Y {
		return math.Sqrt(math.Pow(point.X-lineStart.X, 2) + math.Pow(point.Y-lineStart.Y, 2))
	}

	// Calculate the area of the triangle * 2
	area := math.Abs((lineEnd.Y-lineStart.Y)*point.X - (lineEnd.X-lineStart.X)*point.Y + lineEnd.X*lineStart.Y - lineEnd.Y*lineStart.X)
	
	// Calculate the length of the line segment
	lineLength := math.Sqrt(math.Pow(lineEnd.X-lineStart.X, 2) + math.Pow(lineEnd.Y-lineStart.Y, 2))
	
	// Return the height of the triangle
	return area / lineLength
}

// simplifyPolygon applies the Ramer-Douglas-Peucker algorithm to simplify a polygon
func simplifyPolygon(points []Point, epsilon float64) []Point {
	if len(points) <= 2 {
		return points
	}

	// Find the point with the maximum distance
	maxDistance := 0.0
	maxIndex := 0

	for i := 1; i < len(points)-1; i++ {
		distance := perpendicularDistance(points[i], points[0], points[len(points)-1])
		if distance > maxDistance {
			maxDistance = distance
			maxIndex = i
		}
	}

	// If max distance is greater than epsilon, recursively simplify
	if maxDistance > epsilon {
		// Recursive call
		firstLine := simplifyPolygon(points[:maxIndex+1], epsilon)
		secondLine := simplifyPolygon(points[maxIndex:], epsilon)

		// Combine the results
		return append(firstLine[:len(firstLine)-1], secondLine...)
	}

	// Return the endpoints
	return []Point{points[0], points[len(points)-1]}
}

// simplifyGeoJSONPolygon simplifies a GeoJSON polygon using the Ramer-Douglas-Peucker algorithm
func simplifyGeoJSONPolygon(coordinates [][][]float64) [][][]float64 {
	if len(coordinates) == 0 || len(coordinates[0]) == 0 {
		return coordinates
	}

	// Convert coordinates to points
	points := make([]Point, len(coordinates[0]))
	for i, coord := range coordinates[0] {
		points[i] = Point{X: coord[0], Y: coord[1]}
	}

	// Check if simplification is needed and get appropriate epsilon
	needsSimplification, epsilon := calculatePolygonComplexity(points)
	
	if !needsSimplification {
		return coordinates
	}

	// Simplify the points
	simplifiedPoints := simplifyPolygon(points, epsilon)

	// Convert back to GeoJSON format
	simplifiedCoords := make([][][]float64, 1)
	simplifiedCoords[0] = make([][]float64, len(simplifiedPoints))
	for i, point := range simplifiedPoints {
		simplifiedCoords[0][i] = []float64{point.X, point.Y}
	}

	// Log simplification results
	log.Printf("Polygon simplified from %d to %d points (epsilon: %f)", len(points), len(simplifiedPoints), epsilon)

	// // output the geojson to a file
	// geojsonStr, _ := json.Marshal(simplifiedCoords)
	// os.WriteFile("simplified_geojson.json", geojsonStr, 0644)

	return simplifiedCoords
}

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
		// Simplify the polygon coordinates
		req.Features[0].Geometry.Coordinates = simplifyGeoJSONPolygon(req.Features[0].Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		// Simplify the polygon coordinates
		req.Geometry.Coordinates = simplifyGeoJSONPolygon(req.Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Geometry
	} else {
		http.Error(w, "Invalid GeoJSON type. Must be either 'Feature' or 'FeatureCollection'", http.StatusBadRequest)
		return
	}

	// Search for businesses
	geojsonStr, _ := json.Marshal(geometry)
	businesses, err := h.csvService.SearchBusinesses(string(geojsonStr), req.NAFCode, true)
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

// HandleCompetitorCount handles the competitor count request
func (h *SearchHandler) HandleCompetitorCount(w http.ResponseWriter, r *http.Request) {
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
		// Simplify the polygon coordinates
		req.Features[0].Geometry.Coordinates = simplifyGeoJSONPolygon(req.Features[0].Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		// Simplify the polygon coordinates
		req.Geometry.Coordinates = simplifyGeoJSONPolygon(req.Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Geometry
	} else {
		http.Error(w, "Invalid GeoJSON type. Must be either 'Feature' or 'FeatureCollection'", http.StatusBadRequest)
		return
	}

	// Search for businesses
	geojsonStr, _ := json.Marshal(geometry)
	businesses, err := h.csvService.SearchBusinesses(string(geojsonStr), req.NAFCode, false)
	if err != nil {
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	numberOfCompetitors := models.CompetitorCountResponse{
		NumberOfCompetitors: len(businesses),
	}

	// Return results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(numberOfCompetitors); err != nil {
		http.Error(w, "Error encoding response", http.StatusInternalServerError)
		return
	}

	// Log processing time
	duration := time.Since(startTime)
	log.Printf("Request processed in %v\n", duration)
}

// HandleCompetitionData handles the competition data request
func (h *SearchHandler) HandleCompetitionData(w http.ResponseWriter, r *http.Request) {
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
		// Simplify the polygon coordinates
		req.Features[0].Geometry.Coordinates = simplifyGeoJSONPolygon(req.Features[0].Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		// Simplify the polygon coordinates
		req.Geometry.Coordinates = simplifyGeoJSONPolygon(req.Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Geometry
	} else {
		http.Error(w, "Invalid GeoJSON type. Must be either 'Feature' or 'FeatureCollection'", http.StatusBadRequest)
		return
	}

	// Search for businesses
	geojsonStr, _ := json.Marshal(geometry)
	businesses, err := h.csvService.SearchBusinesses(string(geojsonStr), req.NAFCode, false)
	if err != nil {
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	competitionData, err := h.csvService.GetCompetitionData(businesses)
	if err != nil {
		http.Error(w, "Error processing request", http.StatusInternalServerError)
		return
	}

	// log json response
	jsonResponse, _ := json.Marshal(competitionData)
	log.Printf("competitionData: %s", string(jsonResponse))

	// Return results
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(competitionData); err != nil {
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
		// Simplify the polygon coordinates
		req.Features[0].Geometry.Coordinates = simplifyGeoJSONPolygon(req.Features[0].Geometry.Coordinates) // Adjust epsilon as needed
		geometry = req.Features[0].Geometry
	} else if req.Type == "Feature" {
		if req.Geometry.Type != "Polygon" {
			http.Error(w, "Only Polygon geometry type is supported", http.StatusBadRequest)
			return
		}
		// Simplify the polygon coordinates
		req.Geometry.Coordinates = simplifyGeoJSONPolygon(req.Geometry.Coordinates) // Adjust epsilon as needed
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