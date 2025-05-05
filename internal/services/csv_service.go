package services

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"csv-processor/internal/models"
)

// CSVService handles the business logic for processing the CSV file
type CSVService struct {
	filePath string
}

// NewCSVService creates a new CSVService instance
func NewCSVService(filePath string) *CSVService {
	return &CSVService{
		filePath: filePath,
	}
}

// convertGeoJSONToPoints converts GeoJSON polygon coordinates to our internal Point format
func convertGeoJSONToPoints(coords [][][]float64) []models.Point {
	if len(coords) == 0 || len(coords[0]) == 0 {
		return nil
	}

	points := make([]models.Point, len(coords[0]))
	for i, coord := range coords[0] {
		if len(coord) >= 2 {
			points[i] = models.Point{
				Lat: coord[1], // GeoJSON uses [longitude, latitude]
				Lng: coord[0],
			}
		}
	}
	return points
}

// writeResultsToFile writes the search results to a JSON file
func writeResultsToFile(results []models.Business, nafCode string) error {
	// Create results directory if it doesn't exist
	resultsDir := "results"
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return fmt.Errorf("error creating results directory: %v", err)
	}

	// Generate filename with timestamp and NAF code
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s/results_%s_%s.json", resultsDir, nafCode, timestamp)

	// Create and write to the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating results file: %v", err)
	}
	defer file.Close()

	// Write JSON with indentation
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(results); err != nil {
		return fmt.Errorf("error writing results to file: %v", err)
	}

	log.Printf("Results written to: %s", filename)
	return nil
}

// SearchBusinesses searches for businesses matching the given criteria
func (s *CSVService) SearchBusinesses(req models.SearchRequest) ([]models.Business, error) {
	// Convert GeoJSON polygon to our internal Point format
	var polygon []models.Point
	if len(req.Features) > 0 && req.Features[0].Geometry.Type == "Polygon" {
		polygon = convertGeoJSONToPoints(req.Features[0].Geometry.Coordinates)
	}
	if len(polygon) < 3 {
		return nil, fmt.Errorf("invalid polygon: must have at least 3 points")
	}

	file, err := os.Open(s.filePath)
	if err != nil {
		log.Printf("Error opening CSV file: %v\n", err)
		return nil, fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	// Configure reader to handle line breaks within fields
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1 // Allow variable number of fields

	var results []models.Business
	lineNum := 0

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Printf("Error reading CSV line %d: %v\n", lineNum, err)
			return nil, fmt.Errorf("error reading CSV line %d: %v", lineNum, err)
		}
		lineNum++

		// Skip header row
		if lineNum == 1 || len(record) < 3 || record[0] == "siret" {
			continue
		}

		// Get NAF code (activitePrincipaleEtablissement field)
		nafCode := ""
		for i := len(record) - 1; i >= 0; i-- {
			if strings.Contains(record[i], "NAFRev2") {
				if i > 0 {
					nafCode = record[i-1]
					break
				}
			}
		}

		if nafCode == "" || nafCode != req.NAFCode {
			continue
		}

		// Get coordinates (last two fields)
		if len(record) < 2 {
			continue
		}
		
		longitude, err := strconv.ParseFloat(record[len(record)-2], 64)
		if err != nil {
			log.Printf("Error parsing longitude at line %d: %v\n", lineNum, err)
			continue
		}
		latitude, err := strconv.ParseFloat(record[len(record)-1], 64)
		if err != nil {
			log.Printf("Error parsing latitude at line %d: %v\n", lineNum, err)
			continue
		}

		// Check if point is inside polygon
		if !isPointInPolygon(models.Point{Lat: latitude, Lng: longitude}, polygon) {
			continue
		}

		// Get business name
		businessName := ""
		for i := len(record) - 1; i >= 0; i-- {
			if strings.Contains(record[i], "NAFRev2") {
				if i > 3 {
					businessName = record[i-2]
					break
				}
			}
		}

		// Get address
		address := ""
		// address starts after etablissementSiege which is the 10th field. This field is either True or False, the next field is a number, we skip that, then we have the address
		for i := 11; i < len(record) && i < 20; i++ {
			if record[i] != "" {
				// check if record[i] is a number, if it is, remove decimal point and what is after it
				if _, err := strconv.ParseFloat(record[i], 64); err == nil {
					record[i] = strings.Replace(record[i], ".", " ", -1)
					record[i] = strings.Split(record[i], " ")[0]
				}
				if i >= 12 && i < 17 {
					address += record[i] + " "
				} else if i < 12 || i==17 {
					address += record[i] + ", "
				} else {
					address += record[i] + " "
				}
			}
		}
		if address != "" {
			address = strings.TrimSpace(address)
		}

		// Create business entry
		business := models.Business{
			Name:      businessName,
			NAFCode:   nafCode,
			Latitude:  latitude,
			Longitude: longitude,
			Address:   address,
		}

		results = append(results, business)
	}

	log.Printf("Processed %d lines, found %d matching businesses\n", lineNum, len(results))

	// Write results to file
	if err := writeResultsToFile(results, req.NAFCode); err != nil {
		log.Printf("Warning: Could not write results to file: %v\n", err)
		// Continue anyway, as this is not a critical error
	}

	return results, nil
}

// isPointInPolygon implements the ray casting algorithm for point-in-polygon test
func isPointInPolygon(point models.Point, polygon []models.Point) bool {
	if len(polygon) < 3 {
		return false
	}

	inside := false
	j := len(polygon) - 1

	for i := 0; i < len(polygon); i++ {
		if (polygon[i].Lat > point.Lat) != (polygon[j].Lat > point.Lat) {
			slope := (point.Lng-polygon[i].Lng)*(polygon[j].Lat-polygon[i].Lat) -
				(polygon[j].Lng-polygon[i].Lng)*(point.Lat-polygon[i].Lat)
			if slope == 0 {
				return true
			}
			if (slope < 0) != (polygon[j].Lat < polygon[i].Lat) {
				inside = !inside
			}
		}
		j = i
	}

	return inside
} 