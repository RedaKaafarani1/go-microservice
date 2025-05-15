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
	"sync"
	"time"

	"golang.org/x/exp/slices"

	"csv-processor/internal/models"

	"github.com/twpayne/go-geom"
	// "github.com/twpayne/go-geom/xy"
	geom2 "github.com/peterstace/simplefeatures/geom"
)

// CSVService handles the business logic for processing the CSV file
type CSVService struct {
	businessFilePath string
	irisFilePath    string
	qpFilePath      string
	communeFilePath string
	criminalityService *CriminalityService
}

// NewCSVService creates a new CSVService instance
func NewCSVService(businessFilePath, irisFilePath, qpFilePath, communeFilePath string) *CSVService {
	criminalityService, err := NewCriminalityService()
	if err != nil {
		log.Printf("Warning: failed to initialize criminality service: %v", err)
	}
	
	return &CSVService{
		businessFilePath: businessFilePath,
		irisFilePath:    irisFilePath,
		qpFilePath:      qpFilePath,
		communeFilePath: communeFilePath,
		criminalityService: criminalityService,
	}
}

// convertGeoJSONToPolygon converts GeoJSON polygon coordinates to a go-geom Polygon
func (s *CSVService) convertGeoJSONToPolygon(geojsonStr string) (*geom.Polygon, error) {
	// First try parsing as a regular Polygon
	var polygonGeoJSON struct {
		Type        string          `json:"type"`
		Coordinates [][][]float64   `json:"coordinates"`
	}

	if err := json.Unmarshal([]byte(geojsonStr), &polygonGeoJSON); err == nil && polygonGeoJSON.Type == "Polygon" {
		if len(polygonGeoJSON.Coordinates) == 0 {
			return nil, fmt.Errorf("empty polygon coordinates")
		}

		coords := polygonGeoJSON.Coordinates[0]
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

	// If not a Polygon, try parsing as a MultiPolygon
	var multiPolygonGeoJSON struct {
		Type        string            `json:"type"`
		Coordinates [][][][]float64   `json:"coordinates"`
	}

	if err := json.Unmarshal([]byte(geojsonStr), &multiPolygonGeoJSON); err != nil {
		return nil, fmt.Errorf("error parsing GeoJSON: %v", err)
	}

	if multiPolygonGeoJSON.Type != "MultiPolygon" {
		return nil, fmt.Errorf("unsupported GeoJSON type: %s", multiPolygonGeoJSON.Type)
	}

	if len(multiPolygonGeoJSON.Coordinates) == 0 || len(multiPolygonGeoJSON.Coordinates[0]) == 0 {
		return nil, fmt.Errorf("empty MultiPolygon coordinates")
	}

	// Use the first polygon from the MultiPolygon
	coords := multiPolygonGeoJSON.Coordinates[0][0]
	polygon := geom.NewPolygon(geom.XY)
	coords2D := make([][]geom.Coord, 1)
	coords2D[0] = make([]geom.Coord, len(coords))
	for i, coord := range coords {
		coords2D[0][i] = geom.Coord{coord[0], coord[1]}
	}
	_, err := polygon.SetCoords(coords2D)
	if err != nil {
		return nil, fmt.Errorf("error creating polygon from MultiPolygon: %v", err)
	}
	return polygon, nil
}

func (s *CSVService) convertGeoJSONToGeometry(geojsonStr string) (geom2.Geometry, error) {
	// First try parsing as a regular Polygon
	var polygonGeoJSON struct {
		Type        string          `json:"type"`
		Coordinates [][][]float64   `json:"coordinates"`
	}

	if err := json.Unmarshal([]byte(geojsonStr), &polygonGeoJSON); err == nil && polygonGeoJSON.Type == "Polygon" {
		if len(polygonGeoJSON.Coordinates) == 0 {
			return geom2.Geometry{}, fmt.Errorf("empty polygon coordinates")
		}

		// Convert coordinates to geom2.Geometry format
		coords := polygonGeoJSON.Coordinates[0]
		flatCoords := make([]float64, len(coords)*2)
		for i, coord := range coords {
			flatCoords[i*2] = coord[0]
			flatCoords[i*2+1] = coord[1]
		}
		
		// Create line string from points
		lineString := geom2.NewLineString(geom2.NewSequence(flatCoords, geom2.DimXY))
		if lineString.IsEmpty() {
			return geom2.Geometry{}, fmt.Errorf("error creating line string")
		}

		// Create polygon from line string
		polygon := geom2.NewPolygon([]geom2.LineString{lineString})
		if polygon.IsEmpty() {
			return geom2.Geometry{}, fmt.Errorf("error creating polygon")
		}
		return polygon.AsGeometry(), nil
	}

	// If not a Polygon, try parsing as a MultiPolygon
	var multiPolygonGeoJSON struct {
		Type        string            `json:"type"`
		Coordinates [][][][]float64   `json:"coordinates"`
	}

	if err := json.Unmarshal([]byte(geojsonStr), &multiPolygonGeoJSON); err != nil {
		return geom2.Geometry{}, fmt.Errorf("error parsing GeoJSON: %v", err)
	}

	if multiPolygonGeoJSON.Type != "MultiPolygon" {
		return geom2.Geometry{}, fmt.Errorf("unsupported GeoJSON type: %s", multiPolygonGeoJSON.Type)
	}

	if len(multiPolygonGeoJSON.Coordinates) == 0 || len(multiPolygonGeoJSON.Coordinates[0]) == 0 {
		return geom2.Geometry{}, fmt.Errorf("empty MultiPolygon coordinates")
	}

	// Use the first polygon from the MultiPolygon
	coords := multiPolygonGeoJSON.Coordinates[0][0]
	flatCoords := make([]float64, len(coords)*2)
	for i, coord := range coords {
		flatCoords[i*2] = coord[0]
		flatCoords[i*2+1] = coord[1]
	}

	// Create line string from points
	lineString := geom2.NewLineString(geom2.NewSequence(flatCoords, geom2.DimXY))
	if lineString.IsEmpty() {
		return geom2.Geometry{}, fmt.Errorf("error creating line string")
	}

	// Create polygon from line string
	polygon := geom2.NewPolygon([]geom2.LineString{lineString})
	if polygon.IsEmpty() {
		return geom2.Geometry{}, fmt.Errorf("error creating polygon from MultiPolygon")
	}
	return polygon.AsGeometry(), nil
}

// writeResultsToFile writes the search results to a JSON file
func (s *CSVService) writeResultsToFile(businesses []*models.Business, nafCode string) error {
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
	// Convert GeoJSON to geometry
	geometry, err := s.convertGeoJSONToGeometry(geojsonStr)
	if err != nil {
		return nil, fmt.Errorf("error converting GeoJSON to geometry: %v", err)
	}

	// Load only businesses with matching NAF code
	businesses, err := s.loadBusinessesByNAF(nafCode)
	if err != nil {
		return nil, fmt.Errorf("error loading businesses: %v", err)
	}

	// Create spatial index with filtered businesses
	spatialIndex := models.NewSpatialIndex(businesses)

	// Query businesses within geometry
	results := spatialIndex.Query(geometry)

	// Write results to file
	if err := s.writeResultsToFile(results, nafCode); err != nil {
		log.Printf("Warning: error writing results to file: %v", err)
	}

	return results, nil
}

// loadBusinessesByNAF loads only businesses with the given NAF code
func (s *CSVService) loadBusinessesByNAF(nafCode string) ([]*models.Business, error) {
	file, err := os.Open(s.businessFilePath)
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

// parsePolygon parses a polygon from a GeoJSON string
func (s *CSVService) parsePolygon(polygonStr string) *geom2.Geometry {
	// Remove any leading/trailing quotes
	polygonStr = strings.Trim(polygonStr, "\"")

	polygon, err := s.convertGeoJSONToGeometry(polygonStr)
	if err != nil {
		log.Printf("Error parsing polygon: %v", err)
		return nil
	}
	return &polygon
}

// writeIrisResultsToFile writes the IRIS data results to a JSON file
func (s *CSVService) writeIrisResultsToFile(response *models.IrisResponse) error {
	// Create results directory if it doesn't exist
	resultsDir := "results"
	if err := os.MkdirAll(resultsDir, 0755); err != nil {
		return fmt.Errorf("error creating results directory: %v", err)
	}

	// Generate filename with timestamp
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s/iris_results_%s.json", resultsDir, timestamp)

	// Create and write to the file
	file, err := os.Create(filename)
	if err != nil {
		return fmt.Errorf("error creating results file: %v", err)
	}
	defer file.Close()

	// Write JSON with indentation
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(response); err != nil {
		return fmt.Errorf("error writing results to file: %v", err)
	}

	log.Printf("Results written to: %s", filename)
	return nil
}

func calculateIntersectionArea(requestPoly, irisPoly geom2.Geometry) float64 {
	// First check if they intersect at all - this is the fastest check
	if !geom2.Intersects(requestPoly, irisPoly) {
		return 0
	}

	// Check for exact equality - this is also a fast check
	if equals, err := geom2.Equals(requestPoly, irisPoly); err == nil && equals {
		return irisPoly.Area()
	}

	// Check if one contains the other - this is faster than checking both directions
	if contains, err := geom2.Contains(requestPoly, irisPoly); err == nil && contains {
		return irisPoly.Area()
	}

	// If we get here, we need to calculate the actual intersection area
	intersection, err := geom2.Intersection(requestPoly, irisPoly)
	if err != nil {
		return 0
	}
	return intersection.Area()
}

// calculateIntersectionPercentage calculates the percentage of intersection between two polygons
func calculateIntersectionPercentage(requestPoly, irisPoly *geom2.Geometry) float64 {
	// Calculate intersection area
	intersectionArea := calculateIntersectionArea(*requestPoly, *irisPoly)
	if intersectionArea == 0 {
		return 0
	}
	if intersectionArea == 0 {
		return 0
	}

	// Calculate percentage based on the IRIS polygon's area
	irisArea := irisPoly.Area()
	if irisArea <= 0 {
		return 0
	}
	
	percentage := (intersectionArea / irisArea) * 100

	if percentage < 5 {
		return 0
	}

	return percentage
}

// aggregateIrisData aggregates IRIS data with inclusion percentage
func aggregateIrisData(response *models.IrisResponse, iris *models.IrisData, inclusionPercentage float64) {
	factor := inclusionPercentage / 100.0

	// Aggregate raw data
	for k, v := range iris.RawData {
		response.Data[k] += v * factor
	}

	// Update total area and population
	response.TotalPopulation += iris.TotalPopulation * factor
}

// loadQPData loads QP data from the CSV file
func (s *CSVService) loadQPData() ([]struct {
	ID string
	CodeQP string
	LibQP    string
	Commune  string
	Polygon  *geom2.Geometry
}, error) {
	file, err := os.Open(s.qpFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening QP CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // Set semicolon as delimiter
	reader.LazyQuotes = true

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	var qpData []struct {
		ID string
		CodeQP string
		LibQP    string
		Commune  string
		Polygon  *geom2.Geometry
	}

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(record) < 8 { // We need at least 8 columns
			continue
		}

		// Parse QP data
		id := record[0]
		codeQP := record[1]
		libQP := record[2]    // Lib_QP
		commune := record[3]  // Commune
		polygonStr := record[6] // polygon

		if polygonStr == "" {
			continue
		}

		polygon := s.parsePolygon(polygonStr)
		if polygon == nil {
			continue
		}

		qpData = append(qpData, struct {
			ID string
			CodeQP string
			LibQP    string
			Commune  string
			Polygon  *geom2.Geometry
		}{
			ID: id,
			CodeQP: codeQP,
			LibQP:    libQP,
			Commune:  commune,
			Polygon:  polygon,
		})
	}

	return qpData, nil
}

// loadCommuneData loads commune data from the CSV file for specific commune codes
func (s *CSVService) loadCommuneData(communeCodes map[string]bool) (map[string]*models.CommuneData, error) {
	file, err := os.Open(s.communeFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening commune CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // Set semicolon as delimiter
	reader.LazyQuotes = true

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	communeData := make(map[string]*models.CommuneData)
	lineNumber := 0
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		if len(record) < 7 { // Make sure we have enough columns
			continue
		}

		communeCode := record[0]
		// Skip if this commune is not in our target set
		if !communeCodes[communeCode] {
			continue
		}

		// Create CommuneData struct with values from the record
		communeData[communeCode] = &models.CommuneData{
			// id is the line number
			ID: strconv.Itoa(lineNumber),
			CommuneCode: communeCode,
			Population: parseFloat(record[1]),
			CommuneName: record[len(record)-5],
			PostalCode:  record[len(record)-6],
			SurfaceArea: parseFloat(record[len(record)-4]),
			Polygon: s.parsePolygon(record[len(record)-10]),
		}
		lineNumber++
	}

	return communeData, nil
}

// GetIrisData retrieves and aggregates IRIS data for the given polygon
func (s *CSVService) GetIrisData(geojsonStr string) (*models.IrisResponse, error) {
	// Convert GeoJSON to polygon
	polygon, err := s.convertGeoJSONToGeometry(geojsonStr)
	if err != nil {
		return nil, fmt.Errorf("error converting GeoJSON to polygon: %v", err)
	}

	if polygon.IsEmpty() {
		return nil, fmt.Errorf("failed to create polygon from GeoJSON")
	}

	// Load IRIS data
	irisData, err := s.loadIrisData()
	if err != nil {
		return nil, fmt.Errorf("error loading IRIS data: %v", err)
	}

	// Initialize response with IRIS data
	response := &models.IrisResponse{
		Data: make(map[string]float64),
		Administrative: models.AdministrativeData{
			Communes:     make([]models.CommuneData, 0),
			PostalCodes:  make([]models.PostalCodeData, 0),
			SpecialZones: make([]models.QPData, 0),
		},
	}

	// Create channels for results and errors
	type result struct {
		iris *models.IrisData
		percentage float64
	}
	results := make(chan result, len(irisData))
	errors := make(chan error, len(irisData))

	// Create a WaitGroup to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Process IRIS zones in parallel
	for _, iris := range irisData {
		wg.Add(1)
		go func(iris *models.IrisData) {
			defer wg.Done()
			if iris.Polygon == nil {
				return
			}

			// Calculate intersection percentage
			inclusionPercentage := calculateIntersectionPercentage(&polygon, iris.Polygon)
			if inclusionPercentage > 0 {
				results <- result{iris: iris, percentage: inclusionPercentage}
			}
		}(iris)
	}

	// Close results channel when all goroutines are done
	go func() {
		wg.Wait()
		close(results)
		close(errors)
	}()

	// Track intersecting communes to load only relevant ones
	intersectingCommunes := make(map[string]bool)
	intersectingZones := 0

	// Process results
	for result := range results {
		intersectingZones++
		// Aggregate data with inclusion percentage
		aggregateIrisData(response, result.iris, result.percentage)
		// Track this commune for later processing
		intersectingCommunes[result.iris.COM] = true
	}

	// Check for errors
	for err := range errors {
		if err != nil {
			return nil, fmt.Errorf("error processing IRIS data: %v", err)
		}
	}

	if intersectingZones == 0 {
		return nil, fmt.Errorf("no intersecting zones found")
	}

	// Load only relevant commune data
	communeData, err := s.loadCommuneData(intersectingCommunes)
	if err != nil {
		return nil, fmt.Errorf("error loading commune data: %v", err)
	}

	// Process communes that had intersecting IRIS zones
	for communeCode := range intersectingCommunes {
		if communeValue, exists := communeData[communeCode]; exists {
			communeInclusionPercentage := calculateIntersectionPercentage(&polygon, communeValue.Polygon)
			if communeInclusionPercentage == 0 {
				continue
			}
			// append only if it's not already in the array
			if !slices.Contains(response.Administrative.Communes, *communeValue) {
				communeValue.Percentage = communeInclusionPercentage
				response.Administrative.Communes = append(response.Administrative.Communes, *communeValue)
				// add postal code data
				response.Administrative.PostalCodes = append(response.Administrative.PostalCodes, models.PostalCodeData{
					PostalCode: communeValue.PostalCode,
					Percentage: communeInclusionPercentage,
				})
			}
		}
	}

	// Load and process QP data
	qpData, err := s.loadQPData()
	if err != nil {
		return nil, fmt.Errorf("error loading QP data: %v", err)
	}

	// Process QP data
	for _, qp := range qpData {
		if qp.Polygon == nil {
			continue
		}

		// Calculate intersection percentage
		inclusionPercentage := calculateIntersectionPercentage(&polygon, qp.Polygon)
		if inclusionPercentage == 0 {
			continue
		}
		if inclusionPercentage > 0 {
			response.Administrative.SpecialZones = append(response.Administrative.SpecialZones, models.QPData{
				ID: qp.ID,
				CodeQP: qp.CodeQP,
				LibQP: qp.LibQP,
				Commune: qp.Commune,
				IntersectionPercentage: inclusionPercentage,
			})
		}
	}

	// Calculate criminality data if service is available
	if s.criminalityService != nil {
		response.Criminality = *s.criminalityService.CalculateCriminality(response.Administrative.Communes)
	}

	log.Printf("Found %d intersecting zones", intersectingZones)

	// Write results to file
	if err := s.writeIrisResultsToFile(response); err != nil {
		log.Printf("Warning: error writing results to file: %v", err)
	}

	return response, nil
}

// loadIrisData loads IRIS data from the CSV file
func (s *CSVService) loadIrisData() ([]*models.IrisData, error) {
	file, err := os.Open(s.irisFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening IRIS CSV file: %v", err)
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';' // Set semicolon as delimiter
	reader.LazyQuotes = true

	// Skip header
	_, err = reader.Read()
	if err != nil {
		return nil, fmt.Errorf("error reading CSV header: %v", err)
	}

	var irisData []*models.IrisData
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			continue
		}

		iris := s.parseIrisRecord(record)
		if iris != nil {
			irisData = append(irisData, iris)
		}
	}

	return irisData, nil
}

// parseIrisRecord parses a single IRIS record from the CSV
func (s *CSVService) parseIrisRecord(record []string) *models.IrisData {
	if len(record) < 119 { // We need at least 119 columns
		return nil
	}

	iris := &models.IrisData{
		RawData: make(map[string]float64),
	}

	// Store all raw values except IRIS, COM, TYP_IRIS, LAB_IRIS
	iris.COM = record[1]
	iris.RawData["population_total"] = parseFloat(record[4])
	iris.RawData["population_general_age_0002"] = parseFloat(record[5])
	iris.RawData["population_general_age_0305"] = parseFloat(record[6])
	iris.RawData["population_general_age_0610"] = parseFloat(record[7])
	iris.RawData["population_general_age_1117"] = parseFloat(record[8])
	iris.RawData["population_general_age_1824"] = parseFloat(record[9])
	iris.RawData["population_general_age_2539"] = parseFloat(record[10])
	iris.RawData["population_general_age_4054"] = parseFloat(record[11])
	iris.RawData["population_general_age_5564"] = parseFloat(record[12])
	iris.RawData["population_general_age_6579"] = parseFloat(record[13])
	iris.RawData["population_general_age_80P"] = parseFloat(record[14])
	iris.RawData["population_total_age_0014"] = parseFloat(record[15])
	iris.RawData["population_total_age_1529"] = parseFloat(record[16])
	iris.RawData["population_total_age_3044"] = parseFloat(record[17])
	iris.RawData["population_total_age_4559"] = parseFloat(record[18])
	iris.RawData["population_total_age_6074"] = parseFloat(record[19])
	iris.RawData["population_total_age_75P"] = parseFloat(record[20])
	iris.RawData["population_total_age_0019"] = parseFloat(record[21])
	iris.RawData["population_total_age_2064"] = parseFloat(record[22])
	iris.RawData["population_total_age_65P"] = parseFloat(record[23])
	iris.RawData["population_male"] = parseFloat(record[24])
	iris.RawData["population_male_age_0014"] = parseFloat(record[25])
	iris.RawData["population_male_age_1529"] = parseFloat(record[26])
	iris.RawData["population_male_age_3044"] = parseFloat(record[27])
	iris.RawData["population_male_age_4559"] = parseFloat(record[28])
	iris.RawData["population_male_age_6074"] = parseFloat(record[29])
	iris.RawData["population_male_age_75P"] = parseFloat(record[30])
	iris.RawData["population_male_age_0019"] = parseFloat(record[31])
	iris.RawData["population_male_age_2064"] = parseFloat(record[32])
	iris.RawData["population_male_age_65P"] = parseFloat(record[33])
	iris.RawData["population_female"] = parseFloat(record[34])
	iris.RawData["population_female_age_0014"] = parseFloat(record[35])
	iris.RawData["population_female_age_1529"] = parseFloat(record[36])
	iris.RawData["population_female_age_3044"] = parseFloat(record[37])
	iris.RawData["population_female_age_4559"] = parseFloat(record[38])
	iris.RawData["population_female_age_6074"] = parseFloat(record[39])
	iris.RawData["population_female_age_75P"] = parseFloat(record[40])
	iris.RawData["population_female_age_0019"] = parseFloat(record[41])
	iris.RawData["population_female_age_2064"] = parseFloat(record[42])
	iris.RawData["population_female_age_65P"] = parseFloat(record[43])
	iris.RawData["employees_number"] = parseFloat(record[44])
	iris.RawData["employees_category_1"] = parseFloat(record[45])
	iris.RawData["employees_category_2"] = parseFloat(record[46])
	iris.RawData["employees_category_3"] = parseFloat(record[47])
	iris.RawData["employees_category_4"] = parseFloat(record[48])
	iris.RawData["employees_category_5"] = parseFloat(record[49])
	iris.RawData["employees_category_6"] = parseFloat(record[50])
	iris.RawData["employees_category_7"] = parseFloat(record[51])
	iris.RawData["employees_category_8"] = parseFloat(record[52])
	iris.RawData["employees_male"] = parseFloat(record[53])
	iris.RawData["employees_male_category_1"] = parseFloat(record[54])
	iris.RawData["employees_male_category_2"] = parseFloat(record[55])
	iris.RawData["employees_male_category_3"] = parseFloat(record[56])
	iris.RawData["employees_male_category_4"] = parseFloat(record[57])
	iris.RawData["employees_male_category_5"] = parseFloat(record[58])
	iris.RawData["employees_male_category_6"] = parseFloat(record[59])
	iris.RawData["employees_male_category_7"] = parseFloat(record[60])
	iris.RawData["employees_male_category_8"] = parseFloat(record[61])
	iris.RawData["employees_female"] = parseFloat(record[62])
	iris.RawData["employees_female_category_1"] = parseFloat(record[63])
	iris.RawData["employees_female_category_2"] = parseFloat(record[64])
	iris.RawData["employees_female_category_3"] = parseFloat(record[65])
	iris.RawData["employees_female_category_4"] = parseFloat(record[66])
	iris.RawData["employees_female_category_5"] = parseFloat(record[67])
	iris.RawData["employees_female_category_6"] = parseFloat(record[68])
	iris.RawData["employees_female_category_7"] = parseFloat(record[69])
	iris.RawData["employees_female_category_8"] = parseFloat(record[70])
	iris.RawData["population_french"] = parseFloat(record[71])
	iris.RawData["population_foreign"] = parseFloat(record[72])
	iris.RawData["population_immigrant"] = parseFloat(record[73])
	iris.RawData["housing_people_per_home"] = parseFloat(record[74]) // this should be fixed, people in households
	iris.RawData["housing_people_in_collective_housing"] = parseFloat(record[75])

	// Find the polygon column (column 77)
	polygonStr := record[76] // POLYGON
	if polygonStr == "" {
		return nil
	}

	iris.Polygon = s.parsePolygon(polygonStr)
	if iris.Polygon == nil {
		return nil
	}

	iris.Area = parseFloat(record[77]) // AREA

	iris.RawData["families_only_number"] = parseFloat(record[78])
	iris.RawData["families_with_kids"] = parseFloat(record[79])
	iris.RawData["families_monoparental"] = parseFloat(record[80])
	iris.RawData["families_without_kids"] = parseFloat(record[81])
	iris.RawData["families_with_1_kids_under_25"] = parseFloat(record[82])
	iris.RawData["families_with_2_kids_under_25"] = parseFloat(record[83])
	iris.RawData["families_with_3_kids_under_25"] = parseFloat(record[84])
	iris.RawData["families_with_4p_kids_under_25"] = parseFloat(record[85])
	iris.RawData["families_number"] = parseFloat(record[86])
	iris.RawData["families_one_person"] = parseFloat(record[87])
	iris.RawData["families_living_without_family"] = parseFloat(record[88])
	iris.RawData["families_living_with_family"] = parseFloat(record[89])
	iris.RawData["employees_number"] = parseFloat(record[90])
	iris.RawData["students_number"] = parseFloat(record[91])
	iris.RawData["housing_total"] = parseFloat(record[92])
	iris.RawData["housing_primary_residence"] = parseFloat(record[93])
	iris.RawData["housing_secondary_residence"] = parseFloat(record[94])
	iris.RawData["housing_empty_residence"] = parseFloat(record[95])
	iris.RawData["housing_houses"] = parseFloat(record[96])
	iris.RawData["housing_apartments"] = parseFloat(record[97])
	iris.RawData["housing_rooms_1_rooms"] = parseFloat(record[98])
	iris.RawData["housing_rooms_2_rooms"] = parseFloat(record[99])
	iris.RawData["housing_rooms_3_rooms"] = parseFloat(record[100])
	iris.RawData["housing_rooms_4_rooms"] = parseFloat(record[101])
	iris.RawData["housing_rooms_5p_rooms"] = parseFloat(record[102])
	iris.RawData["housing_houses_constructed_before_19"] = parseFloat(record[103])
	iris.RawData["housing_houses_constructed_19_45"] = parseFloat(record[104])
	iris.RawData["housing_houses_constructed_46_70"] = parseFloat(record[105])
	iris.RawData["housing_houses_constructed_71_90"] = parseFloat(record[106])
	iris.RawData["housing_houses_constructed_91_05"] = parseFloat(record[107])
	iris.RawData["housing_houses_constructed_06_17"] = parseFloat(record[108])
	iris.RawData["housing_moved_since_0_2_years"] = parseFloat(record[109])
	iris.RawData["housing_moved_since_2_4_years"] = parseFloat(record[110])
	iris.RawData["housing_moved_since_5_9_years"] = parseFloat(record[111])
	iris.RawData["housing_moved_since_10p_years"] = parseFloat(record[112])
	iris.RawData["housing_owners"] = parseFloat(record[113])
	iris.RawData["housing_renters"] = parseFloat(record[114])
	iris.RawData["housing_with_parkings"] = parseFloat(record[115])
	iris.RawData["housing_with_atleast_1_cars"] = parseFloat(record[116])
	iris.RawData["housing_with_1_cars"] = parseFloat(record[117])
	iris.RawData["housing_with_2p_cars"] = parseFloat(record[118])

	// Set total population
	iris.TotalPopulation = iris.RawData["population_total"]

	return iris
}

// Helper function to parse float values
func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
} 