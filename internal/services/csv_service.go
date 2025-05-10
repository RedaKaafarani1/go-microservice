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
	"github.com/twpayne/go-geom/xy"
)

// CSVService handles the business logic for processing the CSV file
type CSVService struct {
	businessFilePath string
	irisFilePath    string
	qpFilePath      string
}

// NewCSVService creates a new CSVService instance
func NewCSVService(businessFilePath, irisFilePath, qpFilePath string) *CSVService {
	return &CSVService{
		businessFilePath: businessFilePath,
		irisFilePath:    irisFilePath,
		qpFilePath:      qpFilePath,
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
func (s *CSVService) parsePolygon(polygonStr string) *geom.Polygon {
	// Remove any leading/trailing quotes
	polygonStr = strings.Trim(polygonStr, "\"")

	polygon, err := s.convertGeoJSONToPolygon(polygonStr)
	if err != nil {
		log.Printf("Error parsing polygon: %v", err)
		return nil
	}
	return polygon
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

// calculateIntersectionArea calculates the area of intersection between two polygons
func calculateIntersectionArea(requestPoly, irisPoly *geom.Polygon) float64 {
	// Get the bounds of both polygons
	bounds1 := requestPoly.Bounds()
	bounds2 := irisPoly.Bounds()

	// Quick check if polygons don't overlap
	if !bounds1.Overlaps(geom.XY, bounds2) {
		return 0
	}

	// Get the outer rings of both polygons
	requestRing := requestPoly.LinearRing(0)
	irisRing := irisPoly.LinearRing(0)

	// Create intersection polygon
	intersection := geom.NewPolygon(geom.XY)
	
	// Calculate intersection using geom's built-in functionality
	coords := make([]geom.Coord, 0)
	for _, coord := range irisRing.Coords() {
		if xy.IsPointInRing(geom.XY, coord, requestRing.FlatCoords()) {
			coords = append(coords, coord)
		}
	}

	// If we have intersection points, create the intersection polygon
	if len(coords) > 0 {
		coords = append(coords, coords[0]) // Close the ring
		intersection.MustSetCoords([][]geom.Coord{coords})
		return intersection.Area()
	}

	return 0
}

// calculateIntersectionPercentage calculates the percentage of intersection between two polygons
func calculateIntersectionPercentage(requestPoly, irisPoly *geom.Polygon) float64 {
	// Calculate intersection area
	intersectionArea := calculateIntersectionArea(requestPoly, irisPoly)
	if intersectionArea == 0 {
		return 0
	}

	// Calculate percentage based on the IRIS polygon's area
	irisArea := irisPoly.Area()
	if irisArea <= 0 {
		return 0
	}

	// Calculate percentage
	return (intersectionArea / irisArea) * 100
}

// aggregateIrisData aggregates IRIS data with inclusion percentage
func aggregateIrisData(response *models.IrisResponse, iris *models.IrisData, inclusionPercentage float64) {
	factor := inclusionPercentage / 100.0

	// Aggregate raw data
	for k, v := range iris.RawData {
		response.Data[k] += v * factor
	}

	// Update total area and population
	response.TotalArea += iris.Area * factor
	response.TotalPopulation += iris.TotalPopulation * factor
}

// loadQPData loads QP data from the CSV file
func (s *CSVService) loadQPData() ([]struct {
	LibQP    string
	Commune  string
	Polygon  *geom.Polygon
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
		LibQP    string
		Commune  string
		Polygon  *geom.Polygon
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
			LibQP    string
			Commune  string
			Polygon  *geom.Polygon
		}{
			LibQP:    libQP,
			Commune:  commune,
			Polygon:  polygon,
		})
	}

	return qpData, nil
}

// GetIrisData retrieves and aggregates IRIS data for the given polygon
func (s *CSVService) GetIrisData(geojsonStr string) (*models.IrisResponse, error) {
	// Convert GeoJSON to polygon
	polygon, err := s.convertGeoJSONToPolygon(geojsonStr)
	if err != nil {
		return nil, fmt.Errorf("error converting GeoJSON to polygon: %v", err)
	}

	if polygon == nil {
		return nil, fmt.Errorf("failed to create polygon from GeoJSON")
	}

	// Load IRIS data
	irisData, err := s.loadIrisData()
	if err != nil {
		return nil, fmt.Errorf("error loading IRIS data: %v", err)
	}

	// Load QP data
	qpData, err := s.loadQPData()
	if err != nil {
		return nil, fmt.Errorf("error loading QP data: %v", err)
	}

	// Calculate intersection and aggregate data
	response := &models.IrisResponse{
		Data: make(map[string]float64),
		QPData: make([]models.QPData, 0),
	}

	// Process each IRIS zone
	intersectingZones := 0
	for _, iris := range irisData {
		if iris.Polygon == nil {
			continue
		}

		// Calculate intersection percentage
		inclusionPercentage := calculateIntersectionPercentage(polygon, iris.Polygon)
		if inclusionPercentage > 0 {
			intersectingZones++
			// Aggregate data with inclusion percentage
			aggregateIrisData(response, iris, inclusionPercentage)
		}
	}

	// Process QP data
	for _, qp := range qpData {
		if qp.Polygon == nil {
			continue
		}

		// Calculate intersection percentage
		inclusionPercentage := calculateIntersectionPercentage(polygon, qp.Polygon)
		if inclusionPercentage > 0 {
			response.QPData = append(response.QPData, models.QPData{
				LibQP: qp.LibQP,
				Commune: qp.Commune,
				IntersectionPercentage: inclusionPercentage,
			})
		}
	}

	if intersectingZones == 0 {
		return nil, fmt.Errorf("no intersecting zones found")
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
	iris.RawData["P20_POP"] = parseFloat(record[4])
	iris.RawData["P20_POP0002"] = parseFloat(record[5])
	iris.RawData["P20_POP0305"] = parseFloat(record[6])
	iris.RawData["P20_POP0610"] = parseFloat(record[7])
	iris.RawData["P20_POP1117"] = parseFloat(record[8])
	iris.RawData["P20_POP1824"] = parseFloat(record[9])
	iris.RawData["P20_POP2539"] = parseFloat(record[10])
	iris.RawData["P20_POP4054"] = parseFloat(record[11])
	iris.RawData["P20_POP5564"] = parseFloat(record[12])
	iris.RawData["P20_POP6579"] = parseFloat(record[13])
	iris.RawData["P20_POP80P"] = parseFloat(record[14])
	iris.RawData["P20_POP0014"] = parseFloat(record[15])
	iris.RawData["P20_POP1529"] = parseFloat(record[16])
	iris.RawData["P20_POP3044"] = parseFloat(record[17])
	iris.RawData["P20_POP4559"] = parseFloat(record[18])
	iris.RawData["P20_POP6074"] = parseFloat(record[19])
	iris.RawData["P20_POP75P"] = parseFloat(record[20])
	iris.RawData["P20_POP0019"] = parseFloat(record[21])
	iris.RawData["P20_POP2064"] = parseFloat(record[22])
	iris.RawData["P20_POP65P"] = parseFloat(record[23])
	iris.RawData["P20_POPH"] = parseFloat(record[24])
	iris.RawData["P20_H0014"] = parseFloat(record[25])
	iris.RawData["P20_H1529"] = parseFloat(record[26])
	iris.RawData["P20_H3044"] = parseFloat(record[27])
	iris.RawData["P20_H4559"] = parseFloat(record[28])
	iris.RawData["P20_H6074"] = parseFloat(record[29])
	iris.RawData["P20_H75P"] = parseFloat(record[30])
	iris.RawData["P20_H0019"] = parseFloat(record[31])
	iris.RawData["P20_H2064"] = parseFloat(record[32])
	iris.RawData["P20_H65P"] = parseFloat(record[33])
	iris.RawData["P20_POPF"] = parseFloat(record[34])
	iris.RawData["P20_F0014"] = parseFloat(record[35])
	iris.RawData["P20_F1529"] = parseFloat(record[36])
	iris.RawData["P20_F3044"] = parseFloat(record[37])
	iris.RawData["P20_F4559"] = parseFloat(record[38])
	iris.RawData["P20_F6074"] = parseFloat(record[39])
	iris.RawData["P20_F75P"] = parseFloat(record[40])
	iris.RawData["P20_F0019"] = parseFloat(record[41])
	iris.RawData["P20_F2064"] = parseFloat(record[42])
	iris.RawData["P20_F65P"] = parseFloat(record[43])
	iris.RawData["C20_POP15P"] = parseFloat(record[44])
	iris.RawData["C20_POP15P_CS1"] = parseFloat(record[45])
	iris.RawData["C20_POP15P_CS2"] = parseFloat(record[46])
	iris.RawData["C20_POP15P_CS3"] = parseFloat(record[47])
	iris.RawData["C20_POP15P_CS4"] = parseFloat(record[48])
	iris.RawData["C20_POP15P_CS5"] = parseFloat(record[49])
	iris.RawData["C20_POP15P_CS6"] = parseFloat(record[50])
	iris.RawData["C20_POP15P_CS7"] = parseFloat(record[51])
	iris.RawData["C20_POP15P_CS8"] = parseFloat(record[52])
	iris.RawData["C20_H15P"] = parseFloat(record[53])
	iris.RawData["C20_H15P_CS1"] = parseFloat(record[54])
	iris.RawData["C20_H15P_CS2"] = parseFloat(record[55])
	iris.RawData["C20_H15P_CS3"] = parseFloat(record[56])
	iris.RawData["C20_H15P_CS4"] = parseFloat(record[57])
	iris.RawData["C20_H15P_CS5"] = parseFloat(record[58])
	iris.RawData["C20_H15P_CS6"] = parseFloat(record[59])
	iris.RawData["C20_H15P_CS7"] = parseFloat(record[60])
	iris.RawData["C20_H15P_CS8"] = parseFloat(record[61])
	iris.RawData["C20_F15P"] = parseFloat(record[62])
	iris.RawData["C20_F15P_CS1"] = parseFloat(record[63])
	iris.RawData["C20_F15P_CS2"] = parseFloat(record[64])
	iris.RawData["C20_F15P_CS3"] = parseFloat(record[65])
	iris.RawData["C20_F15P_CS4"] = parseFloat(record[66])
	iris.RawData["C20_F15P_CS5"] = parseFloat(record[67])
	iris.RawData["C20_F15P_CS6"] = parseFloat(record[68])
	iris.RawData["C20_F15P_CS7"] = parseFloat(record[69])
	iris.RawData["C20_F15P_CS8"] = parseFloat(record[70])
	iris.RawData["P20_POP_FR"] = parseFloat(record[71])
	iris.RawData["P20_POP_ETR"] = parseFloat(record[72])
	iris.RawData["P20_POP_IMM"] = parseFloat(record[73])
	iris.RawData["P20_PMEN"] = parseFloat(record[74])
	iris.RawData["P20_PHORMEN"] = parseFloat(record[75])

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
	iris.RawData["AREA"] = iris.Area

	iris.RawData["C20_FAM"] = parseFloat(record[78])
	iris.RawData["C20_COUPAENF"] = parseFloat(record[79])
	iris.RawData["C20_FAMMONO"] = parseFloat(record[80])
	iris.RawData["C20_COUPSENF"] = parseFloat(record[81])
	iris.RawData["C20_NE24F1"] = parseFloat(record[82])
	iris.RawData["C20_NE24F2"] = parseFloat(record[83])
	iris.RawData["C20_NE24F3"] = parseFloat(record[84])
	iris.RawData["C20_NE24F4P"] = parseFloat(record[85])
	iris.RawData["C20_MEN"] = parseFloat(record[86])
	iris.RawData["C20_MENPSEUL"] = parseFloat(record[87])
	iris.RawData["C20_MENSFAM"] = parseFloat(record[88])
	iris.RawData["C20_MENFAM"] = parseFloat(record[89])
	iris.RawData["P20_ACTOCC"] = parseFloat(record[90])
	iris.RawData["P20_ETUD1564"] = parseFloat(record[91])
	iris.RawData["P20_LOG"] = parseFloat(record[92])
	iris.RawData["P20_RP"] = parseFloat(record[93])
	iris.RawData["P20_RSECOCC"] = parseFloat(record[94])
	iris.RawData["P20_LOGVAC"] = parseFloat(record[95])
	iris.RawData["P20_MAISON"] = parseFloat(record[96])
	iris.RawData["P20_APPART"] = parseFloat(record[97])
	iris.RawData["P20_RP_1P"] = parseFloat(record[98])
	iris.RawData["P20_RP_2P"] = parseFloat(record[99])
	iris.RawData["P20_RP_3P"] = parseFloat(record[100])
	iris.RawData["P20_RP_4P"] = parseFloat(record[101])
	iris.RawData["P20_RP_5PP"] = parseFloat(record[102])
	iris.RawData["P20_RP_ACH19"] = parseFloat(record[103])
	iris.RawData["P20_RP_ACH45"] = parseFloat(record[104])
	iris.RawData["P20_RP_ACH70"] = parseFloat(record[105])
	iris.RawData["P20_RP_ACH90"] = parseFloat(record[106])
	iris.RawData["P20_RP_ACH05"] = parseFloat(record[107])
	iris.RawData["P20_RP_ACH17"] = parseFloat(record[108])
	iris.RawData["P20_PMEN_ANEM0002"] = parseFloat(record[109])
	iris.RawData["P20_PMEN_ANEM0204"] = parseFloat(record[110])
	iris.RawData["P20_PMEN_ANEM0509"] = parseFloat(record[111])
	iris.RawData["P20_PMEN_ANEM10P"] = parseFloat(record[112])
	iris.RawData["P20_RP_PROP"] = parseFloat(record[113])
	iris.RawData["P20_RP_LOC"] = parseFloat(record[114])
	iris.RawData["P20_RP_GARL"] = parseFloat(record[115])
	iris.RawData["P20_RP_VOIT1P"] = parseFloat(record[116])
	iris.RawData["P20_RP_VOIT1"] = parseFloat(record[117])
	iris.RawData["P20_RP_VOIT2P"] = parseFloat(record[118])

	// Set total population
	iris.TotalPopulation = iris.RawData["P20_POP"]

	return iris
}

// Helper function to parse float values
func parseFloat(s string) float64 {
	f, _ := strconv.ParseFloat(s, 64)
	return f
} 