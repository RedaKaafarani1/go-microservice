package services

import (
	"encoding/csv"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	// "log"

	"csv-processor/internal/config"
	"csv-processor/internal/models"
	// "golang.org/x/exp/slices"
)

// CompetitionHandler handles competition data requests
type CompetitionService struct {
	competitionData map[string]map[string]string
}

func NewCompetitionService() (*CompetitionService, error) {
	service := &CompetitionService{
		competitionData: make(map[string]map[string]string),
	}

	return service, nil
}

func (s *CompetitionService) doLoadCompetitionData(businesses []*models.Business) error {
	if err := s.loadCompetitionData(businesses); err != nil {
		return err
	}
	return nil
}

func getLatitudeAndLongitude(geolocalisation string) (float64, float64) {
	// geolocalisation is a string like "48.8566,2.3522"
	latitude, err := strconv.ParseFloat(strings.Split(geolocalisation, ",")[0], 64)
	if err != nil {
		return 0.0, 0.0
	}
	longitude, err := strconv.ParseFloat(strings.Split(geolocalisation, ",")[1], 64)
	if err != nil {
		return 0.0, 0.0
	}
	return latitude, longitude
}

func (s *CompetitionService) loadCompetitionData(businesses []*models.Business) error {
	csvConfig := config.GetCSVConfig()
	file, err := os.Open(config.GetDataFilePath(csvConfig.CompetitionData))
	if err != nil {
		return err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = ';'

	header, err := reader.Read()
	if err != nil {
		return err
	}

	//create a map of sirets
	sirets := make(map[string]bool)
	for _, business := range businesses {
		sirets[business.Siret] = true
	}

	for {
		record, err := reader.Read()
		if err != nil {
			break
		}

		if len(record) < len(header) {
			continue
		}

		siren := record[1]
		nic := record[2]
		siret := siren + nic

		// check if siret is in businesses, loads only necessary data
		if _, exists := sirets[siret]; !exists {
			continue
		}

		// remove siret from sirets to avoid duplicates
		delete(sirets, siret)

		if _, exists := s.competitionData[siret]; !exists {
			s.competitionData[siret] = make(map[string]string)
		} else {
			// read publication date
			publicationDate := record[18]
			// check if publication date is more recent than the existing element with the same siret
			const layout = "2006-01-02"
			publicationDateParsed, err := time.Parse(layout, publicationDate)
			if err != nil {
				return err
			}
			// check if publication date is already in the map
			if _, exists := s.competitionData[siret]["publicationDate"]; exists {
				existingPublicationDate, err := time.Parse(layout, s.competitionData[siret]["publicationDate"])
				if err != nil {
					return err
				}
				if !publicationDateParsed.After(existingPublicationDate) {
					continue
				}
			}
		}
		
		name := record[0]
		legalStatus := record[3]
		codeAPE := record[4]
		labelAPE := record[5]
		address := record[6]
		postalCode := record[7]
		city := record[8]
		numDepartment := record[9]
		department := record[10]
		region := record[11]
		codeGreffe := record[12]
		greffe := record[13]
		registrationDate := record[14]
		deregistrationDate := record[15]
		status := record[16]
		geolocalisation := record[17]
		// get latitude and longitude from geolocalisation
		latitude, longitude := getLatitudeAndLongitude(geolocalisation)
		publicationDate := record[18]
		millesime1 := record[19]
		dateCloseEx1 := record[20]
		durationEx1 := record[21]
		ca1 := record[22]
		result1 := record[23]
		employees1 := record[24]
		millesime2 := record[25]
		dateCloseEx2 := record[26]
		durationEx2 := record[27]
		ca2 := record[28]
		result2 := record[29]
		employees2 := record[30]
		millesime3 := record[31]
		dateCloseEx3 := record[32]
		durationEx3 := record[33]
		ca3 := record[34]
		result3 := record[35]
		employees3 := record[36]
		rangeCA1 := record[37]
		rangeCA2 := record[38]
		rangeCA3 := record[39]

		s.competitionData[siret]["name"] = name
		s.competitionData[siret]["siren"] = siren
		s.competitionData[siret]["nic"] = nic
		s.competitionData[siret]["legalStatus"] = legalStatus
		s.competitionData[siret]["codeAPE"] = codeAPE
		s.competitionData[siret]["labelAPE"] = labelAPE
		s.competitionData[siret]["address"] = address
		s.competitionData[siret]["postalCode"] = postalCode
		s.competitionData[siret]["city"] = city
		s.competitionData[siret]["numDepartment"] = numDepartment
		s.competitionData[siret]["department"] = department
		s.competitionData[siret]["region"] = region
		s.competitionData[siret]["codeGreffe"] = codeGreffe
		s.competitionData[siret]["greffe"] = greffe
		s.competitionData[siret]["registrationDate"] = registrationDate
		s.competitionData[siret]["deregistrationDate"] = deregistrationDate
		s.competitionData[siret]["status"] = status
		s.competitionData[siret]["latitude"] = strconv.FormatFloat(latitude, 'f', -1, 64)
		s.competitionData[siret]["longitude"] = strconv.FormatFloat(longitude, 'f', -1, 64)
		s.competitionData[siret]["publicationDate"] = publicationDate
		s.competitionData[siret]["millesime1"] = millesime1
		s.competitionData[siret]["dateCloseEx1"] = dateCloseEx1
		s.competitionData[siret]["durationEx1"] = durationEx1
		s.competitionData[siret]["ca1"] = ca1
		s.competitionData[siret]["result1"] = result1
		s.competitionData[siret]["employees1"] = employees1
		s.competitionData[siret]["millesime2"] = millesime2
		s.competitionData[siret]["dateCloseEx2"] = dateCloseEx2
		s.competitionData[siret]["durationEx2"] = durationEx2
		s.competitionData[siret]["ca2"] = ca2
		s.competitionData[siret]["result2"] = result2
		s.competitionData[siret]["employees2"] = employees2
		s.competitionData[siret]["millesime3"] = millesime3
		s.competitionData[siret]["dateCloseEx3"] = dateCloseEx3
		s.competitionData[siret]["durationEx3"] = durationEx3
		s.competitionData[siret]["ca3"] = ca3
		s.competitionData[siret]["result3"] = result3
		s.competitionData[siret]["employees3"] = employees3
		s.competitionData[siret]["rangeCA1"] = rangeCA1
		s.competitionData[siret]["rangeCA2"] = rangeCA2
		s.competitionData[siret]["rangeCA3"] = rangeCA3
	}

	return nil
}
	
func (s *CompetitionService) GetCompetitionData(businesses []*models.Business) (*models.CompetitionResponseByNAF, error) {
	// Group businesses by NAF code
	businessesByNAF := make(map[string][]*models.Business)
	for _, business := range businesses {
		businessesByNAF[business.NAFCode] = append(businessesByNAF[business.NAFCode], business)
	}

	// Create response structure
	response := &models.CompetitionResponseByNAF{
		NAFCodes: make([]models.NAFCodeCompetitionResponse, 0, len(businessesByNAF)),
	}

	// Process each NAF code group
	for nafCode, nafBusinesses := range businessesByNAF {
		// Create competitors list for this NAF code
		competitors := make([]models.CompetitorsData, 0, len(nafBusinesses))
		
		// Process each business in this NAF code group
		for _, business := range nafBusinesses {
			siret := business.Siret
			if _, exists := s.competitionData[siret]; !exists {
				continue
			}
			currBusiness := s.competitionData[siret]
			
			competitor := models.CompetitorsData{
				Name:      currBusiness["name"],
				Siret:     siret,
				Latitude:  parseFloat(currBusiness["latitude"]),
				Longitude: parseFloat(currBusiness["longitude"]),
			}
			competitors = append(competitors, competitor)
		}

		// Calculate competition stats for this NAF code group
		stats := models.CompetitionResponse{
			NumCompetitorsWithAStatus: 0,
			NumCompetitorsWithBStatus: 0,
			NumCompetitorsWithCStatus: 0,
			NumCompetitorsWithDStatus: 0,
			NumCompetitorsWithEStatus: 0,
			CompetitorsAverageCALastYear: 0,
			CompetitorsAverageCA2YearsAgo: 0,
			CompetitorsAverageCA3YearsAgo: 0,
			CompetitorsAverageRevenueLastYear: 0,
			CompetitorsAverageEmployeesLastYear: 0,
			CompetitorsAverageRevenue2YearsAgo: 0,
			CompetitorsAverageEmployees2YearsAgo: 0,
			CompetitorsAverageRevenue3YearsAgo: 0,
			CompetitorsAverageEmployees3YearsAgo: 0,
			PercentageCompetitorsWithDeclaredCALastYear: 0,
			PercentageCompetitorsWithDeclaredCA2YearsAgo: 0,
			PercentageCompetitorsWithDeclaredCA3YearsAgo: 0,
			PercentageCompetitorsWithDeclaredRevenueLastYear: 0,
			PercentageCompetitorsWithDeclaredEmployeesLastYear: 0,
			PercentageCompetitorsWithDeclaredRevenue2YearsAgo: 0,
			PercentageCompetitorsWithDeclaredEmployees2YearsAgo: 0,
			PercentageCompetitorsWithDeclaredRevenue3YearsAgo: 0,
			PercentageCompetitorsWithDeclaredEmployees3YearsAgo: 0,
			RevenueArrayLastYear: []float64{},
			RevenueArray2YearsAgo: []float64{},
			RevenueArray3YearsAgo: []float64{},
			EmployeesArrayLastYear: []float64{},
			EmployeesArray2YearsAgo: []float64{},
			EmployeesArray3YearsAgo: []float64{},
			NumCompetitorsWithConsistentIncrease: 0,
			NumCompetitorsWithConsistentDecrease: 0,
			NumCompetitorsWithMixedTrend: 0,
			OldDataUsed: false,
		}

		numCompetitorsWithDeclaredRevenueLastYear := 0
		numCompetitorsWithDeclaredRevenue2YearsAgo := 0
		numCompetitorsWithDeclaredRevenue3YearsAgo := 0
		numCompetitorsWithDeclaredEmployeesLastYear := 0
		numCompetitorsWithDeclaredEmployees2YearsAgo := 0
		numCompetitorsWithDeclaredEmployees3YearsAgo := 0
		numCompetitorsWithDeclaredCALastYear := 0
		numCompetitorsWithDeclaredCA2YearsAgo := 0
		numCompetitorsWithDeclaredCA3YearsAgo := 0
		numCompetitors := len(nafBusinesses)

		for _, business := range nafBusinesses {
			siret := business.Siret
			if _, exists := s.competitionData[siret]; !exists {
				continue
			}
			currBusiness := s.competitionData[siret]

			currKey := ""

			if currBusiness["rangeCA1"] != "" {
				currKey = "rangeCA1"
			} else if currBusiness["rangeCA2"] != "" {
				currKey = "rangeCA2"
			} else if currBusiness["rangeCA3"] != "" {
				currKey = "rangeCA3"
			}

			if currKey != "rangeCA1" {
				stats.OldDataUsed = true
			}

			if currKey != "" {
				if currBusiness[currKey][0] == 'A' {
					stats.NumCompetitorsWithAStatus++
				} else if currBusiness[currKey][0] == 'B' {
					stats.NumCompetitorsWithBStatus++
				} else if currBusiness[currKey][0] == 'C' {
					stats.NumCompetitorsWithCStatus++
				} else if currBusiness[currKey][0] == 'D' {
					stats.NumCompetitorsWithDStatus++
				} else if currBusiness[currKey][0] == 'E' {
					stats.NumCompetitorsWithEStatus++
				}
			}

			// Process CA data
			ca1 := ""
			ca2 := ""
			ca3 := ""

			if currBusiness["ca1"] != "" && currBusiness["ca1"] != "Confidentiel" {
				ca1 = currBusiness["ca1"]
			}
			if currBusiness["ca2"] != "" && currBusiness["ca2"] != "Confidentiel" {
				ca2 = currBusiness["ca2"]
			}
			if currBusiness["ca3"] != "" && currBusiness["ca3"] != "Confidentiel" {
				ca3 = currBusiness["ca3"]
			}

			if ca1 != "" {
				numCompetitorsWithDeclaredCALastYear++
				stats.CAArrayLastYear = append(stats.CAArrayLastYear, parseFloat(ca1))
				stats.CompetitorsAverageCALastYear += parseFloat(ca1)
			}
			if ca2 != "" {
				numCompetitorsWithDeclaredCA2YearsAgo++
				stats.CAArray2YearsAgo = append(stats.CAArray2YearsAgo, parseFloat(ca2))
				stats.CompetitorsAverageCA2YearsAgo += parseFloat(ca2)
			}
			if ca3 != "" {
				numCompetitorsWithDeclaredCA3YearsAgo++
				stats.CAArray3YearsAgo = append(stats.CAArray3YearsAgo, parseFloat(ca3))
				stats.CompetitorsAverageCA3YearsAgo += parseFloat(ca3)
			}

			// Check if CA values are consistent
			if ca1 != "" && ca2 != "" && ca3 != "" {
				if ca1 > ca2 && ca2 > ca3 {
					stats.NumCompetitorsWithConsistentIncrease++
				} else if ca1 < ca2 && ca2 < ca3 {
					stats.NumCompetitorsWithConsistentDecrease++
				} else {
					stats.NumCompetitorsWithMixedTrend++
				}
			}

			// Process revenue data
			revenue1 := ""
			revenue2 := ""
			revenue3 := ""

			if currBusiness["result1"] != "" && currBusiness["result1"] != "Confidentiel" {
				revenue1 = currBusiness["result1"]
			}
			if currBusiness["result2"] != "" && currBusiness["result2"] != "Confidentiel" {
				revenue2 = currBusiness["result2"]
			}
			if currBusiness["result3"] != "" && currBusiness["result3"] != "Confidentiel" {
				revenue3 = currBusiness["result3"]
			}

			if revenue1 != "" {
				numCompetitorsWithDeclaredRevenueLastYear++
				stats.RevenueArrayLastYear = append(stats.RevenueArrayLastYear, parseFloat(revenue1))
				stats.CompetitorsAverageRevenueLastYear += parseFloat(revenue1)
			}
			if revenue2 != "" {
				numCompetitorsWithDeclaredRevenue2YearsAgo++
				stats.RevenueArray2YearsAgo = append(stats.RevenueArray2YearsAgo, parseFloat(revenue2))
				stats.CompetitorsAverageRevenue2YearsAgo += parseFloat(revenue2)
			}
			if revenue3 != "" {
				numCompetitorsWithDeclaredRevenue3YearsAgo++
				stats.RevenueArray3YearsAgo = append(stats.RevenueArray3YearsAgo, parseFloat(revenue3))
				stats.CompetitorsAverageRevenue3YearsAgo += parseFloat(revenue3)
			}

			// Process employees data
			employees1 := ""
			employees2 := ""
			employees3 := ""

			if currBusiness["employees1"] != "" && currBusiness["employees1"] != "Confidentiel" {
				stats.EmployeesArrayLastYear = append(stats.EmployeesArrayLastYear, parseFloat(currBusiness["employees1"]))
				employees1 = currBusiness["employees1"]
			}
			if currBusiness["employees2"] != "" && currBusiness["employees2"] != "Confidentiel" {
				stats.EmployeesArray2YearsAgo = append(stats.EmployeesArray2YearsAgo, parseFloat(currBusiness["employees2"]))
				employees2 = currBusiness["employees2"]
			}
			if currBusiness["employees3"] != "" && currBusiness["employees3"] != "Confidentiel" {
				stats.EmployeesArray3YearsAgo = append(stats.EmployeesArray3YearsAgo, parseFloat(currBusiness["employees3"]))
				employees3 = currBusiness["employees3"]
			}

			if employees1 != "" {
				numCompetitorsWithDeclaredEmployeesLastYear++
				stats.CompetitorsAverageEmployeesLastYear += int(parseFloat(employees1))
			}
			if employees2 != "" {
				numCompetitorsWithDeclaredEmployees2YearsAgo++
				stats.CompetitorsAverageEmployees2YearsAgo += int(parseFloat(employees2))
			}
			if employees3 != "" {
				numCompetitorsWithDeclaredEmployees3YearsAgo++
				stats.CompetitorsAverageEmployees3YearsAgo += int(parseFloat(employees3))
			}
		}

		// Calculate percentages and averages
		if numCompetitors > 0 {
			stats.PercentageCompetitorsWithDeclaredCALastYear = float64(numCompetitorsWithDeclaredCALastYear) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredCA2YearsAgo = float64(numCompetitorsWithDeclaredCA2YearsAgo) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredCA3YearsAgo = float64(numCompetitorsWithDeclaredCA3YearsAgo) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredRevenueLastYear = float64(numCompetitorsWithDeclaredRevenueLastYear) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredEmployeesLastYear = float64(numCompetitorsWithDeclaredEmployeesLastYear) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredRevenue2YearsAgo = float64(numCompetitorsWithDeclaredRevenue2YearsAgo) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredEmployees2YearsAgo = float64(numCompetitorsWithDeclaredEmployees2YearsAgo) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredRevenue3YearsAgo = float64(numCompetitorsWithDeclaredRevenue3YearsAgo) / float64(numCompetitors) * 100
			stats.PercentageCompetitorsWithDeclaredEmployees3YearsAgo = float64(numCompetitorsWithDeclaredEmployees3YearsAgo) / float64(numCompetitors) * 100
		}

		if numCompetitorsWithDeclaredCALastYear > 0 {
			stats.CompetitorsAverageCALastYear = math.Round(stats.CompetitorsAverageCALastYear / float64(numCompetitorsWithDeclaredCALastYear))
		}
		if numCompetitorsWithDeclaredCA2YearsAgo > 0 {
			stats.CompetitorsAverageCA2YearsAgo = math.Round(stats.CompetitorsAverageCA2YearsAgo / float64(numCompetitorsWithDeclaredCA2YearsAgo))
		}
		if numCompetitorsWithDeclaredCA3YearsAgo > 0 {
			stats.CompetitorsAverageCA3YearsAgo = math.Round(stats.CompetitorsAverageCA3YearsAgo / float64(numCompetitorsWithDeclaredCA3YearsAgo))
		}

		if numCompetitorsWithDeclaredRevenueLastYear > 0 {
			stats.CompetitorsAverageRevenueLastYear = math.Round(stats.CompetitorsAverageRevenueLastYear / float64(numCompetitorsWithDeclaredRevenueLastYear))
		}
		if numCompetitorsWithDeclaredRevenue2YearsAgo > 0 {
			stats.CompetitorsAverageRevenue2YearsAgo = math.Round(stats.CompetitorsAverageRevenue2YearsAgo / float64(numCompetitorsWithDeclaredRevenue2YearsAgo))
		}
		if numCompetitorsWithDeclaredRevenue3YearsAgo > 0 {
			stats.CompetitorsAverageRevenue3YearsAgo = math.Round(stats.CompetitorsAverageRevenue3YearsAgo / float64(numCompetitorsWithDeclaredRevenue3YearsAgo))
		}

		if numCompetitorsWithDeclaredEmployeesLastYear > 0 {
			stats.CompetitorsAverageEmployeesLastYear = int(math.Round(float64(stats.CompetitorsAverageEmployeesLastYear) / float64(numCompetitorsWithDeclaredEmployeesLastYear)))
		}
		if numCompetitorsWithDeclaredEmployees2YearsAgo > 0 {
			stats.CompetitorsAverageEmployees2YearsAgo = int(math.Round(float64(stats.CompetitorsAverageEmployees2YearsAgo) / float64(numCompetitorsWithDeclaredEmployees2YearsAgo)))
		}
		if numCompetitorsWithDeclaredEmployees3YearsAgo > 0 {
			stats.CompetitorsAverageEmployees3YearsAgo = int(math.Round(float64(stats.CompetitorsAverageEmployees3YearsAgo) / float64(numCompetitorsWithDeclaredEmployees3YearsAgo)))
		}

		// Add this NAF code's data to the response
		response.NAFCodes = append(response.NAFCodes, models.NAFCodeCompetitionResponse{
			NAFCode:           nafCode,
			NumberOfCompetitors: len(competitors),
			Competitors:       competitors,
			CompetitionStats:  stats,
		})
	}

	return response, nil
}
	