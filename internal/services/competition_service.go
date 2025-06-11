package services

import (
	"encoding/csv"
	"os"
	"strconv"
	"strings"
	"time"

	"csv-processor/internal/config"
	"csv-processor/internal/models"
	"golang.org/x/exp/slices"
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
		if !slices.ContainsFunc(businesses, func(b *models.Business) bool {
			return b.Siret == siret
		}) {
			continue
		}
		

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
	
func (s *CompetitionService) GetCompetitionData(businesses []*models.Business) (*models.CompetitionResponse, error) {
	response := models.CompetitionResponse{
		NumCompetitorsWithAStatus: 0,
		NumCompetitorsWithBStatus: 0,
		NumCompetitorsWithCStatus: 0,
		NumCompetitorsWithDStatus: 0,
		NumCompetitorsWithEStatus: 0,
		CompetitorsAverageRevenueLastYear: 0,
		CompetitorsAverageEmployeesLastYear: 0,
		CompetitorsAverageRevenue2YearsAgo: 0,
		CompetitorsAverageEmployees2YearsAgo: 0,
		CompetitorsAverageRevenue3YearsAgo: 0,
		CompetitorsAverageEmployees3YearsAgo: 0,
		PercentageCompetitorsWithDeclaredRevenueLastYear: 0,
		PercentageCompetitorsWithDeclaredEmployeesLastYear: 0,
		PercentageCompetitorsWithDeclaredRevenue2YearsAgo: 0,
		PercentageCompetitorsWithDeclaredEmployees2YearsAgo: 0,
		PercentageCompetitorsWithDeclaredRevenue3YearsAgo: 0,
		PercentageCompetitorsWithDeclaredEmployees3YearsAgo: 0,
		OldDataUsed: false,
	}

	numCompetitorsWithDeclaredRevenueLastYear := 0
	numCompetitorsWithDeclaredRevenue2YearsAgo := 0
	numCompetitorsWithDeclaredRevenue3YearsAgo := 0
	numCompetitorsWithDeclaredEmployeesLastYear := 0
	numCompetitorsWithDeclaredEmployees2YearsAgo := 0
	numCompetitorsWithDeclaredEmployees3YearsAgo := 0
	numCompetitors := len(businesses)
	
	for _, business := range businesses {
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
			response.OldDataUsed = true
		}

		if currKey != "" {
			if currBusiness[currKey][0] == 'A' {
				response.NumCompetitorsWithAStatus++
			} else if currBusiness[currKey][0] == 'B' {
				response.NumCompetitorsWithBStatus++
			} else if currBusiness[currKey][0] == 'C' {
				response.NumCompetitorsWithCStatus++
			} else if currBusiness[currKey][0] == 'D' {
				response.NumCompetitorsWithDStatus++
			} else if currBusiness[currKey][0] == 'E' {
				response.NumCompetitorsWithEStatus++
			}
		}
		
		// Handle revenue data
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
			response.CompetitorsAverageRevenueLastYear += parseFloat(revenue1)
		}
		if revenue2 != "" {
			numCompetitorsWithDeclaredRevenue2YearsAgo++
			response.CompetitorsAverageRevenue2YearsAgo += parseFloat(revenue2)
		}
		if revenue3 != "" {
			numCompetitorsWithDeclaredRevenue3YearsAgo++
			response.CompetitorsAverageRevenue3YearsAgo += parseFloat(revenue3)
		}

		// Handle employees data
		employees1 := ""
		employees2 := ""
		employees3 := ""

		if currBusiness["employees1"] != "" && currBusiness["employees1"] != "Confidentiel" {
			employees1 = currBusiness["employees1"]
		}
		if currBusiness["employees2"] != "" && currBusiness["employees2"] != "Confidentiel" {
			employees2 = currBusiness["employees2"]
		}
		if currBusiness["employees3"] != "" && currBusiness["employees3"] != "Confidentiel" {
			employees3 = currBusiness["employees3"]
		}

		if employees1 != "" {
			numCompetitorsWithDeclaredEmployeesLastYear++
			response.CompetitorsAverageEmployeesLastYear += parseFloat(employees1)
		}
		if employees2 != "" {
			numCompetitorsWithDeclaredEmployees2YearsAgo++
			response.CompetitorsAverageEmployees2YearsAgo += parseFloat(employees2)
		}
		if employees3 != "" {
			numCompetitorsWithDeclaredEmployees3YearsAgo++
			response.CompetitorsAverageEmployees3YearsAgo += parseFloat(employees3)
		}
	}

	// Calculate percentages and averages after the loop
	if numCompetitors > 0 {
		response.PercentageCompetitorsWithDeclaredRevenueLastYear = float64(numCompetitorsWithDeclaredRevenueLastYear) / float64(numCompetitors) * 100
		response.PercentageCompetitorsWithDeclaredEmployeesLastYear = float64(numCompetitorsWithDeclaredEmployeesLastYear) / float64(numCompetitors) * 100
		response.PercentageCompetitorsWithDeclaredRevenue2YearsAgo = float64(numCompetitorsWithDeclaredRevenue2YearsAgo) / float64(numCompetitors) * 100
		response.PercentageCompetitorsWithDeclaredEmployees2YearsAgo = float64(numCompetitorsWithDeclaredEmployees2YearsAgo) / float64(numCompetitors) * 100
		response.PercentageCompetitorsWithDeclaredRevenue3YearsAgo = float64(numCompetitorsWithDeclaredRevenue3YearsAgo) / float64(numCompetitors) * 100
		response.PercentageCompetitorsWithDeclaredEmployees3YearsAgo = float64(numCompetitorsWithDeclaredEmployees3YearsAgo) / float64(numCompetitors) * 100
	}
	
	if numCompetitorsWithDeclaredRevenueLastYear > 0 {
		response.CompetitorsAverageRevenueLastYear = response.CompetitorsAverageRevenueLastYear / float64(numCompetitorsWithDeclaredRevenueLastYear)
	}
	if numCompetitorsWithDeclaredRevenue2YearsAgo > 0 {
		response.CompetitorsAverageRevenue2YearsAgo = response.CompetitorsAverageRevenue2YearsAgo / float64(numCompetitorsWithDeclaredRevenue2YearsAgo)
	}
	if numCompetitorsWithDeclaredRevenue3YearsAgo > 0 {
		response.CompetitorsAverageRevenue3YearsAgo = response.CompetitorsAverageRevenue3YearsAgo / float64(numCompetitorsWithDeclaredRevenue3YearsAgo)
	}

	if numCompetitorsWithDeclaredEmployeesLastYear > 0 {
		response.CompetitorsAverageEmployeesLastYear = response.CompetitorsAverageEmployeesLastYear / float64(numCompetitorsWithDeclaredEmployeesLastYear)
	}
	if numCompetitorsWithDeclaredEmployees2YearsAgo > 0 {
		response.CompetitorsAverageEmployees2YearsAgo = response.CompetitorsAverageEmployees2YearsAgo / float64(numCompetitorsWithDeclaredEmployees2YearsAgo)
	}
	if numCompetitorsWithDeclaredEmployees3YearsAgo > 0 {
		response.CompetitorsAverageEmployees3YearsAgo = response.CompetitorsAverageEmployees3YearsAgo / float64(numCompetitorsWithDeclaredEmployees3YearsAgo)
	}

	return &response, nil
}
	