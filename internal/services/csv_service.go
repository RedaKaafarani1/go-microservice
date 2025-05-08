package services

import (
	"bufio"
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

	"github.com/twpayne/go-geom"
)

// CSVService handles the business logic for processing the CSV file
type CSVService struct {
	filePath string
	spatialIndex *models.SpatialIndex
}

// NewCSVService creates a new CSVService instance
func NewCSVService(filePath string) *CSVService {
	return &CSVService{
		filePath: filePath,
	}
}

// convertGeoJSONToPolygon converts GeoJSON polygon coordinates to a go-geom Polygon
func convertGeoJSONToPolygon(coords [][][]float64) (*geom.Polygon, error) {
	if len(coords) == 0 || len(coords[0]) == 0 {
		return nil, fmt.Errorf("invalid coordinates")
	}

	// Convert coordinates to the format expected by go-geom
	linearRings := make([]*geom.LinearRing, 1)
	coords2D := make([]geom.Coord, len(coords[0]))
	for i, coord := range coords[0] {
		if len(coord) < 2 {
			return nil, fmt.Errorf("invalid coordinate at index %d", i)
		}
		coords2D[i] = geom.Coord{coord[0], coord[1]}
	}

	linearRing, err := geom.NewLinearRing(geom.XY).SetCoords(coords2D)
	if err != nil {
		return nil, fmt.Errorf("error creating linear ring: %v", err)
	}
	linearRings[0] = linearRing

	// Convert coordinates to flat format
	flatCoords := make([]float64, 0, len(coords2D)*2)
	ends := make([]int, 1)
	for _, coord := range coords2D {
		flatCoords = append(flatCoords, coord[0], coord[1])
	}
	ends[0] = len(flatCoords)
	return geom.NewPolygonFlat(geom.XY, flatCoords, ends), nil
}

// writeResultsToFile writes the search results to a JSON file
func writeResultsToFile(businesses []*models.Business, nafCode string) error {
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
	if err := encoder.Encode(businesses); err != nil {
		return fmt.Errorf("error writing results to file: %v", err)
	}

	return nil
}

// SearchBusinesses searches for businesses matching the given criteria
func (s *CSVService) SearchBusinesses(geojsonStr string, nafCode string) ([]*models.Business, error) {
	// Convert GeoJSON to polygon
	polygon, err := s.convertGeoJSONToPolygon(geojsonStr)
	if err != nil {
		return nil, fmt.Errorf("error converting GeoJSON to polygon: %v", err)
	}

	// Load only businesses with matching NAF code
	businesses, err := s.loadBusinessesByNAF(nafCode)
	if err != nil {
		return nil, fmt.Errorf("error loading businesses: %v", err)
	}

	// Create spatial index with filtered businesses
	spatialIndex := models.NewSpatialIndex(businesses)

	// Query businesses within polygon
	results := spatialIndex.Query(polygon)

	// Write results to file
	if err := s.writeResultsToFile(results, nafCode); err != nil {
		log.Printf("Warning: error writing results to file: %v", err)
	}

	return results, nil
}

// loadBusinessesByNAF loads only businesses with the given NAF code
func (s *CSVService) loadBusinessesByNAF(nafCode string) ([]*models.Business, error) {
	file, err := os.Open(s.filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening CSV file: %v", err)
	}
	defer file.Close()

	// Use buffered reader for better performance
	bufReader := bufio.NewReader(file)
	reader := csv.NewReader(bufReader)
	reader.LazyQuotes = true
	reader.FieldsPerRecord = -1
	reader.ReuseRecord = true // Reuse record slice for better memory usage

	// Pre-allocate slice with reasonable capacity
	businesses := make([]*models.Business, 0, 1000)

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	// Pre-allocate address builder with reasonable capacity
	var address strings.Builder
	address.Grow(200)

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(record) < 20 {
			continue
		}

		// Parse NAF code first to filter early
		recordNAFCode := record[len(record)-5]
		if recordNAFCode != nafCode {
			continue
		}

		// Parse business name
		businessName := record[0]
		if businessName == "" {
			continue
		}

		// Parse coordinates
		longitude, err := strconv.ParseFloat(record[len(record)-2], 64)
		if err != nil {
			continue
		}
		latitude, err := strconv.ParseFloat(record[len(record)-1], 64)
		if err != nil {
			continue
		}

		// Reset address builder
		address.Reset()

		// Parse address more efficiently
		addressParts := []string{
			record[12],  // complementAdresseEtablissement
			record[13],  // numeroVoieEtablissement
			record[17],  // typeVoieEtablissement
			record[18],  // libelleVoieEtablissement
			record[19],  // codePostalEtablissement
			record[20],  // libelleCommuneEtablissement
		}

		for i, part := range addressParts {
			if part != "" {
				if i > 0 {
					address.WriteString(" ")
				}
				address.WriteString(part)
			}
		}

		// Create business entry
		business := &models.Business{
			Name:      businessName,
			NAFCode:   recordNAFCode,
			Latitude:  latitude,
			Longitude: longitude,
			Address:   address.String(),
		}

		businesses = append(businesses, business)
	}

	return businesses, nil
}

func (s *CSVService) convertGeoJSONToPolygon(geojsonStr string) (*geom.Polygon, error) {
	var geojson struct {
		Type        string          `json:"type"`
		Coordinates [][][]float64   `json:"coordinates"`
	}

	if err := json.Unmarshal([]byte(geojsonStr), &geojson); err != nil {
		return nil, fmt.Errorf("error parsing GeoJSON: %v", err)
	}

	if geojson.Type != "Polygon" {
		return nil, fmt.Errorf("unsupported GeoJSON type: %s", geojson.Type)
	}

	if len(geojson.Coordinates) == 0 {
		return nil, fmt.Errorf("empty polygon coordinates")
	}

	// Pre-allocate coords slice with exact size
	coords := make([][]float64, len(geojson.Coordinates[0]))
	for i, coord := range geojson.Coordinates[0] {
		coords[i] = make([]float64, 2)
		copy(coords[i], coord)
	}

	// Create polygon with pre-allocated coordinates
	polygon := geom.NewPolygon(geom.XY)
	coords2D := make([][]geom.Coord, 1)
	coords2D[0] = make([]geom.Coord, len(coords))
	for i, coord := range coords {
		coords2D[0][i] = geom.Coord{coord[0], coord[1]}
	}
	_, err := polygon.SetCoords(coords2D)
	if err != nil {
		return nil, fmt.Errorf("error creating polygon: %v", err)
	}

	return polygon, nil
}

func (s *CSVService) writeResultsToFile(businesses []*models.Business, nafCode string) error {
	return writeResultsToFile(businesses, nafCode)
} 