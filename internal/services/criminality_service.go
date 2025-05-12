package services

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"csv-processor/internal/models"
)

type CriminalityService struct {
	communeCrimes     map[string]map[string]float64 // map[commune_code]map[crime_type]rate
	departmentCrimes  map[string]map[string]float64 // map[department_code]map[crime_type]rate
	departmentPopulations map[string]float64        // map[department_code]population
}

func NewCriminalityService() (*CriminalityService, error) {
	service := &CriminalityService{
		communeCrimes:     make(map[string]map[string]float64),
		departmentCrimes:  make(map[string]map[string]float64),
		departmentPopulations: make(map[string]float64),
	}

	if err := service.loadCommuneCrimes(); err != nil {
		return nil, fmt.Errorf("failed to load commune crimes: %w", err)
	}

	if err := service.loadDepartmentCrimes(); err != nil {
		return nil, fmt.Errorf("failed to load department crimes: %w", err)
	}

	return service, nil
}

func (s *CriminalityService) loadCommuneCrimes() error {
	file, err := os.Open("./data/crimes_per_commune.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Read header to get crime types
	header, err := reader.Read()
	if err != nil {
		return err
	}

	// Skip first column (CODGEO_2023)
	crimeTypes := header[1:]

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < len(header) {
			continue
		}

		communeCode := strings.TrimLeft(record[0], "0")
		if _, exists := s.communeCrimes[communeCode]; !exists {
			s.communeCrimes[communeCode] = make(map[string]float64)
		}

		// Process each crime type
		for i, crimeType := range crimeTypes {
			if i+1 >= len(record) {
				continue
			}
			
			// Only store non-empty values
			if record[i+1] != "" {
				rate, err := strconv.ParseFloat(record[i+1], 64)
				if err != nil {
					continue
				}
				s.communeCrimes[communeCode][crimeType] = rate
			}
		}
	}

	return nil
}

func (s *CriminalityService) loadDepartmentCrimes() error {
	file, err := os.Open("./data/dep-crime-data.csv")
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	// Read header to get crime types
	header, err := reader.Read()
	if err != nil {
		return err
	}

	// Skip first column (Code.département)
	crimeTypes := header[1:]

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < len(header) {
			continue
		}

		departmentCode := strings.TrimLeft(record[0], "0")

		// Initialize department crimes map if not exists
		if _, exists := s.departmentCrimes[departmentCode]; !exists {
			s.departmentCrimes[departmentCode] = make(map[string]float64)
		}

		// Process each crime type
		for i, crimeType := range crimeTypes {
			if i+1 >= len(record) {
				continue
			}
			
			// Only store non-empty values
			if record[i+1] != "" {
				rate, err := strconv.ParseFloat(record[i+1], 64)
				if err != nil {
					continue
				}
				s.departmentCrimes[departmentCode][crimeType] = rate
			}
		}
	}

	return nil
}

func (s *CriminalityService) CalculateCriminality(communes []models.CommuneData) *models.CriminalityResponse {
	response := &models.CriminalityResponse{
		DrugUsage: &models.CriminalityData{},
		VehicleTheft: &models.CriminalityData{},
		ArmedRobberies: &models.CriminalityData{},
		HomeBurglaries: &models.CriminalityData{},
		SexualViolence: &models.CriminalityData{},
		DrugTrafficking: &models.CriminalityData{},
		VoluntaryInjuries: &models.CriminalityData{},
		TheftFromVehicles: &models.CriminalityData{},
		OtherVoluntaryInjuries: &models.CriminalityData{},
		TheftOfVehicleAccessories: &models.CriminalityData{},
		IntrafamilyVoluntaryInjuries: &models.CriminalityData{},
		VoluntaryDamageAndVandalism: &models.CriminalityData{},
		ViolentRobberiesWithoutWeapon: &models.CriminalityData{},
		RobberiesWithoutViolenceAgainstPersons: &models.CriminalityData{},
	}

	// Map to store accumulated crime data
	crimeData := make(map[string]*models.CriminalityData)
	hasData := make(map[string]bool) // Track which crime types have data

	// Track total population and covered population
	totalPopulation := 0.0
	coveredPopulation := 0.0

	// Track department data
	departmentData := make(map[string]struct {
		totalCrimes map[string]float64
		totalPopulation float64
	})

	// Process each commune
	for _, commune := range communes {
		communeCode := strings.TrimLeft(commune.CommuneCode, "0")
		departmentCode := communeCode[:2] // First two digits of INSEE code

		// Initialize department data if not exists
		if _, exists := departmentData[departmentCode]; !exists {
			departmentData[departmentCode] = struct {
				totalCrimes map[string]float64
				totalPopulation float64
			}{
				totalCrimes: make(map[string]float64),
				totalPopulation: 0,
			}
		}

		// Get commune crimes
		communeCrimes, hasCommuneCrimes := s.communeCrimes[communeCode]
		if hasCommuneCrimes {
			// Get population from IRIS data (assuming it's stored in the commune data)
			population := commune.Population * commune.Percentage / 100
			coveredPopulation += population

			for crimeType, rate := range communeCrimes {
				if _, exists := crimeData[crimeType]; !exists {
					crimeData[crimeType] = &models.CriminalityData{
						IsTotal: true,
						CrimesTotal: 0,
						PercentageCoveredCrimes: 100,
					}
				}

				// Calculate weighted crime rate based on population
				weightedCrimes := (rate * population) / 1000 // rate is per 1000 inhabitants
				crimeData[crimeType].CrimesTotal += weightedCrimes
				hasData[crimeType] = true

				// Add to department totals
				deptData := departmentData[departmentCode]
				deptData.totalCrimes[crimeType] += weightedCrimes
				deptData.totalPopulation += population
				departmentData[departmentCode] = deptData
			}
		}

		totalPopulation += commune.Population * commune.Percentage / 100
	}

	// Calculate final rates and compare with department averages
	for crimeType, data := range crimeData {
		if !hasData[crimeType] {
			data.IsTotal = false
			continue
		}

		// Calculate crime rate per 1000 inhabitants for the area
		areaCrimeRate := (data.CrimesTotal * 1000) / totalPopulation

		// Find the department with the highest population coverage
		var maxDeptCode string
		var maxDeptPopulation float64
		for deptCode, deptData := range departmentData {
			if deptData.totalPopulation > maxDeptPopulation {
				maxDeptPopulation = deptData.totalPopulation
				maxDeptCode = deptCode
			}
		}

		// Get department crime rate from the CSV data
		if deptCrimes, exists := s.departmentCrimes[maxDeptCode]; exists {
			if deptRate, exists := deptCrimes[crimeType]; exists {
				// Calculate relative percentage using the department rate from CSV
				data.PercentageRelativeToDepartmental = ((areaCrimeRate - deptRate) / deptRate) * 100
			}
		}

		// Calculate percentage covered
		if totalPopulation > 0 {
			data.PercentageCoveredCrimes = (coveredPopulation / totalPopulation) * 100
		}
	}

	// Map crime data to response fields
	if data, exists := crimeData["drug_usage"]; exists && hasData["drug_usage"] {
		response.DrugUsage = data
	} else {
		response.DrugUsage = nil
	}
	if data, exists := crimeData["vehicle_theft"]; exists && hasData["vehicle_theft"] {
		response.VehicleTheft = data
	} else {
		response.VehicleTheft = nil
	}
	if data, exists := crimeData["armed_robberies"]; exists && hasData["armed_robberies"] {
		response.ArmedRobberies = data
	} else {
		response.ArmedRobberies = nil
	}
	if data, exists := crimeData["home_burglaries"]; exists && hasData["home_burglaries"] {
		response.HomeBurglaries = data
	} else {
		response.HomeBurglaries = nil
	}
	if data, exists := crimeData["sexual_violence"]; exists && hasData["sexual_violence"] {
		response.SexualViolence = data
	} else {
		response.SexualViolence = nil
	}
	if data, exists := crimeData["drug_trafficking"]; exists && hasData["drug_trafficking"] {
		response.DrugTrafficking = data
	} else {
		response.DrugTrafficking = nil
	}
	if data, exists := crimeData["voluntary_injuries"]; exists && hasData["voluntary_injuries"] {
		response.VoluntaryInjuries = data
	} else {
		response.VoluntaryInjuries = nil
	}
	if data, exists := crimeData["theft_from_vehicles"]; exists && hasData["theft_from_vehicles"] {
		response.TheftFromVehicles = data
	} else {
		response.TheftFromVehicles = nil
	}
	if data, exists := crimeData["other_voluntary_injuries"]; exists && hasData["other_voluntary_injuries"] {
		response.OtherVoluntaryInjuries = data
	} else {
		response.OtherVoluntaryInjuries = nil
	}
	if data, exists := crimeData["theft_of_vehicle_accessories"]; exists && hasData["theft_of_vehicle_accessories"] {
		response.TheftOfVehicleAccessories = data
	} else {
		response.TheftOfVehicleAccessories = nil
	}
	if data, exists := crimeData["intrafamily_voluntary_injuries"]; exists && hasData["intrafamily_voluntary_injuries"] {
		response.IntrafamilyVoluntaryInjuries = data
	} else {
		response.IntrafamilyVoluntaryInjuries = nil
	}
	if data, exists := crimeData["voluntary_damage_and_vandalism"]; exists && hasData["voluntary_damage_and_vandalism"] {
		response.VoluntaryDamageAndVandalism = data
	} else {
		response.VoluntaryDamageAndVandalism = nil
	}
	if data, exists := crimeData["violent_robberies_without_weapon"]; exists && hasData["violent_robberies_without_weapon"] {
		response.ViolentRobberiesWithoutWeapon = data
	} else {
		response.ViolentRobberiesWithoutWeapon = nil
	}
	if data, exists := crimeData["robberies_without_violence_against_persons"]; exists && hasData["robberies_without_violence_against_persons"] {
		response.RobberiesWithoutViolenceAgainstPersons = data
	} else {
		response.RobberiesWithoutViolenceAgainstPersons = nil
	}

	return response
} 