package services

import (
	"encoding/csv"
	"fmt"
	"os"
	"strconv"
	"strings"

	"csv-processor/internal/config"
	"csv-processor/internal/models"
)

type CriminalityService struct {
	communeCrimes     map[string]map[string]float64 // map[commune_code]map[crime_type]rate
	departmentCrimes  map[string]map[string]float64 // map[department_code]map[crime_type]rate
}

func NewCriminalityService() (*CriminalityService, error) {
	service := &CriminalityService{
		communeCrimes:     make(map[string]map[string]float64),
		departmentCrimes:  make(map[string]map[string]float64),
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
	csvConfig := config.GetCSVConfig()
	file, err := os.Open(config.GetDataFilePath(csvConfig.CommuneCrimes))
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
			
			// Initialize rate as 0
			rate := 0.0
			
			// If the field is not empty, parse it as float
			if record[i+1] != "" {
				var err error
				rate, err = strconv.ParseFloat(record[i+1], 64)
				if err != nil {
					continue
				}
			}
			
			// Always store the rate, even if it's 0
			s.communeCrimes[communeCode][crimeType] = rate
		}
	}

	return nil
}

func (s *CriminalityService) loadDepartmentCrimes() error {
	csvConfig := config.GetCSVConfig()
	file, err := os.Open(config.GetDataFilePath(csvConfig.DepartmentCrimes))
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

	// Skip first two columns (Code.département and POP)
	crimeTypes := header[2:]

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

		population, err := strconv.ParseFloat(record[1], 64)
		if err != nil {
			continue
		}
		s.departmentCrimes[departmentCode]["population"] = population

		// Process each crime type
		for i, crimeType := range crimeTypes {
			if i+2 >= len(record) {
				continue
			}
			
			// Only store non-empty values
			if record[i+2] != "" {
				rate, err := strconv.ParseFloat(record[i+2], 64)
				if err != nil {
					continue
				}
				s.departmentCrimes[departmentCode][crimeType] = rate
			}
		}
	}

	return nil
}

func (s *CriminalityService) extractDepartmentCodeFromInseeCode(codeInsee string) string {
	// Handle Corsican departments: 2A and 2B are special cases
	if strings.HasPrefix(codeInsee, "20") {
		if len(codeInsee) > 2 && (codeInsee[2] == 'A' || codeInsee[2] == 'B') {
			return codeInsee[:3]
		}
		return "2A"
	}

	// Return the first two characters for most cases (mainland France)
	return codeInsee[:2]
}

func (s *CriminalityService) CalculateCriminality(communes []models.CommuneData) *models.CriminalityResponse {
	response := &models.CriminalityResponse{
		DrugUsage: nil,
		VehicleTheft: nil,
		ArmedRobberies: nil,
		HomeBurglaries: nil,
		SexualViolence: nil,
		DrugTrafficking: nil,
		VoluntaryInjuries: nil,
		TheftFromVehicles: nil,
		OtherVoluntaryInjuries: nil,
		TheftOfVehicleAccessories: nil,
		IntrafamilyVoluntaryInjuries: nil,
		VoluntaryDamageAndVandalism: nil,
		ViolentRobberiesWithoutWeapon: nil,
		RobberiesWithoutViolenceAgainstPersons: nil,
	}

	// Map to store accumulated crime data
	crimeData := make(map[string]*models.CriminalityData)

	// Track department data
	departmentData := make(map[string]struct {
		totalCrimes map[string]float64
		totalPopulation float64
	})

	// Process each commune
	for _, commune := range communes {
		communeCode := strings.TrimLeft(commune.CommuneCode, "0")
		departmentCode := s.extractDepartmentCodeFromInseeCode(communeCode)

		// Load department data
		if departmentCrimes, exists := s.departmentCrimes[departmentCode]; exists {
			data := departmentData[departmentCode]
			if data.totalCrimes == nil {
				data.totalCrimes = make(map[string]float64)
			}
			data.totalCrimes = departmentCrimes
			data.totalPopulation = departmentCrimes["population"]
			departmentData[departmentCode] = data
		}

		// Get commune crimes
		communeCrimes, hasCommuneCrimes := s.communeCrimes[communeCode]
		if hasCommuneCrimes {
			// Calculate population and area for this commune
			population := commune.Population * commune.Percentage / 100
			area := commune.SurfaceArea * commune.Percentage / 100

			for crimeType, rate := range communeCrimes {
				if _, exists := crimeData[crimeType]; !exists {
					crimeData[crimeType] = &models.CriminalityData{
						IsTotal: true,
						CrimesTotal: population * rate / 1000,
						CoveredArea: area,
						PartialCoveredArea: func() float64 {
							if rate > 0 {
								return area
							}
							return 0
						}(),
						CoveredResidence: population,
						PercentageRelativeToDepartmental: 0,
					}
					if rate == 0 {
						crimeData[crimeType].IsTotal = false
					}
				} else {
					if rate == 0 {
						crimeData[crimeType].IsTotal = false
					}
					crimeData[crimeType].CoveredArea += area
					// Only add to PartialCoveredArea if there are crimes (rate > 0)
					if rate > 0 {
						crimeData[crimeType].PartialCoveredArea += area
					}
					crimeData[crimeType].CrimesTotal += population * rate / 1000
					crimeData[crimeType].CoveredResidence += population
				}
			}
		}
	}

	// Sum up department data
	finalDepartmentData := make(map[string]struct {
		totalCrimes map[string]float64
		totalPopulation float64
	})

	for _, data := range departmentData {
		for crimeType, rate := range data.totalCrimes {
			if crimeType == "population" {
				continue
			}
			deptData := finalDepartmentData[crimeType]
			if deptData.totalCrimes == nil {
				deptData.totalCrimes = make(map[string]float64)
			}
			deptData.totalCrimes[crimeType] += rate
			deptData.totalPopulation += data.totalPopulation
			finalDepartmentData[crimeType] = deptData
		}
	}

	// Calculate final rates and percentages
	for crimeType, data := range crimeData {
		// Calculate departmental criminality rate
		departmentalCriminalityRate := func() float64 {
			if deptData, exists := finalDepartmentData[crimeType]; exists {
				if deptData.totalPopulation > 0 {
					return (deptData.totalCrimes[crimeType] * 1000) / deptData.totalPopulation
				}
			}
			return 0
		}()

		// Calculate criminality rate for selected area
		criminalityRateForArea := func() float64 {
			if data.CoveredResidence > 0 {
				return (data.CrimesTotal * 1000) / data.CoveredResidence
			}
			return 0
		}()

		// Calculate percentage covered crimes
		data.PercentageCoveredCrimes = func() float64 {
			if data.CoveredArea > 0 {
				return 100 * data.PartialCoveredArea / data.CoveredArea
			}
			return 0
		}()

		if data.PercentageCoveredCrimes > 0 {
			// Calculate relative percentage to departmental rate
			if departmentalCriminalityRate > 0 {
				data.PercentageRelativeToDepartmental = ((criminalityRateForArea - departmentalCriminalityRate) / departmentalCriminalityRate) * 100
			}

			// Set final crime rate
			data.CrimesTotal = criminalityRateForArea

			// Map to response fields based on crime type
			switch crimeType {
			case "drug_usage":
				response.DrugUsage = data
			case "vehicle_theft":
				response.VehicleTheft = data
			case "armed_robberies":
				response.ArmedRobberies = data
			case "home_burglaries":
				response.HomeBurglaries = data
			case "sexual_violence":
				response.SexualViolence = data
			case "drug_trafficking":
				response.DrugTrafficking = data
			case "voluntary_injuries":
				response.VoluntaryInjuries = data
			case "theft_from_vehicles":
				response.TheftFromVehicles = data
			case "other_voluntary_injuries":
				response.OtherVoluntaryInjuries = data
			case "theft_of_vehicle_accessories":
				response.TheftOfVehicleAccessories = data
			case "intrafamily_voluntary_injuries":
				response.IntrafamilyVoluntaryInjuries = data
			case "voluntary_damage_and_vandalism":
				response.VoluntaryDamageAndVandalism = data
			case "violent_robberies_without_weapon":
				response.ViolentRobberiesWithoutWeapon = data
			case "robberies_without_violence_against_persons":
				response.RobberiesWithoutViolenceAgainstPersons = data
			}
		}
	}

	return response
} 