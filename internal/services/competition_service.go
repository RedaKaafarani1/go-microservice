package services

import (
	"encoding/csv"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"csv-processor/internal/config"
	"csv-processor/internal/models"
	// "golang.org/x/exp/slices"
)

// CompetitionService handles competition data requests
type CompetitionService struct {
	competitionData map[string]*models.BusinessData
}

func NewCompetitionService() (*CompetitionService, error) {
	service := &CompetitionService{
		competitionData: make(map[string]*models.BusinessData),
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
	parts := strings.Split(geolocalisation, ",")
	if len(parts) != 2 {
		return 0.0, 0.0
	}
	latitude, _ := strconv.ParseFloat(parts[0], 64)
	longitude, _ := strconv.ParseFloat(parts[1], 64)
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

	// Create a map of sirets for faster lookup
	sirets := make(map[string]bool, len(businesses))
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

		// Skip if siret not in businesses
		if !sirets[siret] {
			continue
		}

		// Remove siret from sirets to avoid duplicates
		delete(sirets, siret)

		// Check if we need to update existing data
		existingData, exists := s.competitionData[siret]
		if exists {
			publicationDate := record[18]
			const layout = "2006-01-02"
			publicationDateParsed, err := time.Parse(layout, publicationDate)
			if err != nil {
				continue
			}
			if existingData.PublicationDate != "" {
				existingPublicationDate, err := time.Parse(layout, existingData.PublicationDate)
				if err != nil || !publicationDateParsed.After(existingPublicationDate) {
					continue
				}
			}
		}

		// Parse geolocation
		latitude, longitude := getLatitudeAndLongitude(record[17])

		// Create new business data
		businessData := &models.BusinessData{
			Name:               record[0],
			Siren:             siren,
			NIC:               nic,
			LegalStatus:       record[3],
			CodeAPE:           record[4],
			LabelAPE:          record[5],
			Address:           record[6],
			PostalCode:        record[7],
			City:              record[8],
			NumDepartment:     record[9],
			Department:        record[10],
			Region:            record[11],
			CodeGreffe:        record[12],
			Greffe:            record[13],
			RegistrationDate:  record[14],
			DeregistrationDate: record[15],
			Status:            record[16],
			Latitude:          latitude,
			Longitude:         longitude,
			PublicationDate:   record[18],
			Millesime1:        record[19],
			DateCloseEx1:      record[20],
			DurationEx1:       record[21],
			CA1:               record[22],
			Result1:           record[23],
			Employees1:        record[24],
			Millesime2:        record[25],
			DateCloseEx2:      record[26],
			DurationEx2:       record[27],
			CA2:               record[28],
			Result2:           record[29],
			Employees2:        record[30],
			Millesime3:        record[31],
			DateCloseEx3:      record[32],
			DurationEx3:       record[33],
			CA3:               record[34],
			Result3:           record[35],
			Employees3:        record[36],
			RangeCA1:          record[37],
			RangeCA2:          record[38],
			RangeCA3:          record[39],
		}

		s.competitionData[siret] = businessData
	}

	return nil
}

// Helper functions for processing business data
func (s *CompetitionService) processBusinessData(business *models.BusinessData) (float64, float64, float64, float64, float64, float64, float64, float64, float64) {
	var ca1, ca2, ca3, revenue1, revenue2, revenue3, employees1, employees2, employees3 float64

	if business.CA1 != "" && business.CA1 != "Confidentiel" {
		ca1 = parseFloat(business.CA1)
	}
	if business.CA2 != "" && business.CA2 != "Confidentiel" {
		ca2 = parseFloat(business.CA2)
	}
	if business.CA3 != "" && business.CA3 != "Confidentiel" {
		ca3 = parseFloat(business.CA3)
	}

	if business.Result1 != "" && business.Result1 != "Confidentiel" {
		revenue1 = parseFloat(business.Result1)
	}
	if business.Result2 != "" && business.Result2 != "Confidentiel" {
		revenue2 = parseFloat(business.Result2)
	}
	if business.Result3 != "" && business.Result3 != "Confidentiel" {
		revenue3 = parseFloat(business.Result3)
	}

	if business.Employees1 != "" && business.Employees1 != "Confidentiel" {
		employees1 = parseFloat(business.Employees1)
	}
	if business.Employees2 != "" && business.Employees2 != "Confidentiel" {
		employees2 = parseFloat(business.Employees2)
	}
	if business.Employees3 != "" && business.Employees3 != "Confidentiel" {
		employees3 = parseFloat(business.Employees3)
	}

	return ca1, ca2, ca3, revenue1, revenue2, revenue3, employees1, employees2, employees3
}

func (s *CompetitionService) getStatusCount(business *models.BusinessData) (int, int, int, int, int) {
	var a, b, c, d, e int
	currKey := ""

	if business.RangeCA1 != "" {
		currKey = "rangeCA1"
	} else if business.RangeCA2 != "" {
		currKey = "rangeCA2"
	} else if business.RangeCA3 != "" {
		currKey = "rangeCA3"
	}

	if currKey != "" {
		var status string
		switch currKey {
		case "rangeCA1":
			status = business.RangeCA1
		case "rangeCA2":
			status = business.RangeCA2
		case "rangeCA3":
			status = business.RangeCA3
		}

		if len(status) > 0 {
			switch status[0] {
			case 'A':
				a++
			case 'B':
				b++
			case 'C':
				c++
			case 'D':
				d++
			case 'E':
				e++
			}
		}
	}

	return a, b, c, d, e
}

func (s *CompetitionService) calculateAverages(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	sum := 0.0
	for _, v := range values {
		sum += v
	}
	return math.Round(sum / float64(len(values)))
}

func (s *CompetitionService) calculatePercentages(declared, total int) float64 {
	if total == 0 {
		return 0
	}
	return float64(declared) / float64(total) * 100
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

	// Variables to track totals for averages
	totalNumCompetitorsWithAStatus := 0
	totalNumCompetitorsWithBStatus := 0
	totalNumCompetitorsWithCStatus := 0
	totalNumCompetitorsWithDStatus := 0
	totalNumCompetitorsWithEStatus := 0
	totalCompetitorsAverageCALastYear := 0.0
	totalCompetitorsAverageCA2YearsAgo := 0.0
	totalCompetitorsAverageCA3YearsAgo := 0.0
	totalCompetitorsAverageRevenueLastYear := 0.0
	totalCompetitorsAverageEmployeesLastYear := 0
	totalCompetitorsAverageRevenue2YearsAgo := 0.0
	totalCompetitorsAverageEmployees2YearsAgo := 0
	totalCompetitorsAverageRevenue3YearsAgo := 0.0
	totalCompetitorsAverageEmployees3YearsAgo := 0
	totalPercentageCompetitorsWithDeclaredCALastYear := 0.0
	totalPercentageCompetitorsWithDeclaredCA2YearsAgo := 0.0
	totalPercentageCompetitorsWithDeclaredCA3YearsAgo := 0.0
	totalPercentageCompetitorsWithDeclaredRevenueLastYear := 0.0
	totalPercentageCompetitorsWithDeclaredEmployeesLastYear := 0.0
	totalPercentageCompetitorsWithDeclaredRevenue2YearsAgo := 0.0
	totalPercentageCompetitorsWithDeclaredEmployees2YearsAgo := 0.0
	totalPercentageCompetitorsWithDeclaredRevenue3YearsAgo := 0.0
	totalPercentageCompetitorsWithDeclaredEmployees3YearsAgo := 0.0
	totalCAArrayLastYear := []float64{}
	totalCAArray2YearsAgo := []float64{}
	totalCAArray3YearsAgo := []float64{}
	totalRevenueArrayLastYear := []float64{}
	totalRevenueArray2YearsAgo := []float64{}
	totalRevenueArray3YearsAgo := []float64{}
	totalEmployeesArrayLastYear := []float64{}
	totalEmployeesArray2YearsAgo := []float64{}
	totalEmployeesArray3YearsAgo := []float64{}
	totalNumCompetitorsWithConsistentIncrease := 0.0
	totalNumCompetitorsWithConsistentDecrease := 0.0
	totalNumCompetitorsWithMixedTrend := 0.0
	totalOldDataUsed := false

	totalNumCompetitorsWithDeclaredRevenueLastYear := 0
	totalNumCompetitorsWithDeclaredRevenue2YearsAgo := 0
	totalNumCompetitorsWithDeclaredRevenue3YearsAgo := 0
	totalNumCompetitorsWithDeclaredEmployeesLastYear := 0
	totalNumCompetitorsWithDeclaredEmployees2YearsAgo := 0
	totalNumCompetitorsWithDeclaredEmployees3YearsAgo := 0
	totalNumCompetitorsWithDeclaredCALastYear := 0
	totalNumCompetitorsWithDeclaredCA2YearsAgo := 0
	totalNumCompetitorsWithDeclaredCA3YearsAgo := 0
	totalNumCompetitors := len(businesses)

	// Process each NAF code group that has businesses
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
				Name:      currBusiness.Name,
				Siret:     siret,
				Latitude:  currBusiness.Latitude,
				Longitude: currBusiness.Longitude,
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

			if currBusiness.RangeCA1 != "" {
				currKey = "rangeCA1"
			} else if currBusiness.RangeCA2 != "" {
				currKey = "rangeCA2"
			} else if currBusiness.RangeCA3 != "" {
				currKey = "rangeCA3"
			}

			if currKey != "rangeCA1" {
				stats.OldDataUsed = true
			}

			// Use getStatusCount function instead of duplicating logic
			a, b, c, d, e := s.getStatusCount(currBusiness)
			stats.NumCompetitorsWithAStatus += a
			stats.NumCompetitorsWithBStatus += b
			stats.NumCompetitorsWithCStatus += c
			stats.NumCompetitorsWithDStatus += d
			stats.NumCompetitorsWithEStatus += e

			// Process CA data
			ca1, ca2, ca3, revenue1, revenue2, revenue3, employees1, employees2, employees3 := s.processBusinessData(currBusiness)

			if ca1 != 0 {
				numCompetitorsWithDeclaredCALastYear++
				stats.CAArrayLastYear = append(stats.CAArrayLastYear, ca1)
				stats.CompetitorsAverageCALastYear += ca1
			}
			if ca2 != 0 {
				numCompetitorsWithDeclaredCA2YearsAgo++
				stats.CAArray2YearsAgo = append(stats.CAArray2YearsAgo, ca2)
				stats.CompetitorsAverageCA2YearsAgo += ca2
			}
			if ca3 != 0 {
				numCompetitorsWithDeclaredCA3YearsAgo++
				stats.CAArray3YearsAgo = append(stats.CAArray3YearsAgo, ca3)
				stats.CompetitorsAverageCA3YearsAgo += ca3
			}

			// Check if CA values are consistent
			if ca1 != 0 && ca2 != 0 && ca3 != 0 {
				if ca1 > ca2 && ca2 > ca3 {
					stats.NumCompetitorsWithConsistentIncrease++
				} else if ca1 < ca2 && ca2 < ca3 {
					stats.NumCompetitorsWithConsistentDecrease++
				} else {
					stats.NumCompetitorsWithMixedTrend++
				}
			}

			// Process revenue data
			if revenue1 != 0 {
				numCompetitorsWithDeclaredRevenueLastYear++
				stats.RevenueArrayLastYear = append(stats.RevenueArrayLastYear, revenue1)
				stats.CompetitorsAverageRevenueLastYear += revenue1
			}
			if revenue2 != 0 {
				numCompetitorsWithDeclaredRevenue2YearsAgo++
				stats.RevenueArray2YearsAgo = append(stats.RevenueArray2YearsAgo, revenue2)
				stats.CompetitorsAverageRevenue2YearsAgo += revenue2
			}
			if revenue3 != 0 {
				numCompetitorsWithDeclaredRevenue3YearsAgo++
				stats.RevenueArray3YearsAgo = append(stats.RevenueArray3YearsAgo, revenue3)
				stats.CompetitorsAverageRevenue3YearsAgo += revenue3
			}

			// Process employees data
			if employees1 != 0 {
				numCompetitorsWithDeclaredEmployeesLastYear++
				stats.EmployeesArrayLastYear = append(stats.EmployeesArrayLastYear, employees1)
				stats.CompetitorsAverageEmployeesLastYear += int(employees1)
			}
			if employees2 != 0 {
				numCompetitorsWithDeclaredEmployees2YearsAgo++
				stats.EmployeesArray2YearsAgo = append(stats.EmployeesArray2YearsAgo, employees2)
				stats.CompetitorsAverageEmployees2YearsAgo += int(employees2)
			}
			if employees3 != 0 {
				numCompetitorsWithDeclaredEmployees3YearsAgo++
				stats.EmployeesArray3YearsAgo = append(stats.EmployeesArray3YearsAgo, employees3)
				stats.CompetitorsAverageEmployees3YearsAgo += int(employees3)
			}
		}

		// Add to totals for averages
		totalNumCompetitorsWithAStatus += stats.NumCompetitorsWithAStatus
		totalNumCompetitorsWithBStatus += stats.NumCompetitorsWithBStatus
		totalNumCompetitorsWithCStatus += stats.NumCompetitorsWithCStatus
		totalNumCompetitorsWithDStatus += stats.NumCompetitorsWithDStatus
		totalNumCompetitorsWithEStatus += stats.NumCompetitorsWithEStatus
		totalCompetitorsAverageCALastYear += stats.CompetitorsAverageCALastYear
		totalCompetitorsAverageCA2YearsAgo += stats.CompetitorsAverageCA2YearsAgo
		totalCompetitorsAverageCA3YearsAgo += stats.CompetitorsAverageCA3YearsAgo
		totalCompetitorsAverageRevenueLastYear += stats.CompetitorsAverageRevenueLastYear
		totalCompetitorsAverageEmployeesLastYear += stats.CompetitorsAverageEmployeesLastYear
		totalCompetitorsAverageRevenue2YearsAgo += stats.CompetitorsAverageRevenue2YearsAgo
		totalCompetitorsAverageEmployees2YearsAgo += stats.CompetitorsAverageEmployees2YearsAgo
		totalCompetitorsAverageRevenue3YearsAgo += stats.CompetitorsAverageRevenue3YearsAgo
		totalCompetitorsAverageEmployees3YearsAgo += stats.CompetitorsAverageEmployees3YearsAgo
		totalNumCompetitorsWithConsistentIncrease += float64(stats.NumCompetitorsWithConsistentIncrease)
		totalNumCompetitorsWithConsistentDecrease += float64(stats.NumCompetitorsWithConsistentDecrease)
		totalNumCompetitorsWithMixedTrend += float64(stats.NumCompetitorsWithMixedTrend)
		totalOldDataUsed = totalOldDataUsed || stats.OldDataUsed

		totalCAArrayLastYear = append(totalCAArrayLastYear, stats.CAArrayLastYear...)
		totalCAArray2YearsAgo = append(totalCAArray2YearsAgo, stats.CAArray2YearsAgo...)
		totalCAArray3YearsAgo = append(totalCAArray3YearsAgo, stats.CAArray3YearsAgo...)
		totalRevenueArrayLastYear = append(totalRevenueArrayLastYear, stats.RevenueArrayLastYear...)
		totalRevenueArray2YearsAgo = append(totalRevenueArray2YearsAgo, stats.RevenueArray2YearsAgo...)
		totalRevenueArray3YearsAgo = append(totalRevenueArray3YearsAgo, stats.RevenueArray3YearsAgo...)
		totalEmployeesArrayLastYear = append(totalEmployeesArrayLastYear, stats.EmployeesArrayLastYear...)
		totalEmployeesArray2YearsAgo = append(totalEmployeesArray2YearsAgo, stats.EmployeesArray2YearsAgo...)
		totalEmployeesArray3YearsAgo = append(totalEmployeesArray3YearsAgo, stats.EmployeesArray3YearsAgo...)

		totalNumCompetitorsWithDeclaredRevenueLastYear += numCompetitorsWithDeclaredRevenueLastYear
		totalNumCompetitorsWithDeclaredRevenue2YearsAgo += numCompetitorsWithDeclaredRevenue2YearsAgo
		totalNumCompetitorsWithDeclaredRevenue3YearsAgo += numCompetitorsWithDeclaredRevenue3YearsAgo
		totalNumCompetitorsWithDeclaredEmployeesLastYear += numCompetitorsWithDeclaredEmployeesLastYear
		totalNumCompetitorsWithDeclaredEmployees2YearsAgo += numCompetitorsWithDeclaredEmployees2YearsAgo
		totalNumCompetitorsWithDeclaredEmployees3YearsAgo += numCompetitorsWithDeclaredEmployees3YearsAgo
		totalNumCompetitorsWithDeclaredCALastYear += numCompetitorsWithDeclaredCALastYear
		totalNumCompetitorsWithDeclaredCA2YearsAgo += numCompetitorsWithDeclaredCA2YearsAgo
		totalNumCompetitorsWithDeclaredCA3YearsAgo += numCompetitorsWithDeclaredCA3YearsAgo
		totalNumCompetitors += numCompetitors

		// Calculate percentages and averages
		if numCompetitors > 0 {
			stats.PercentageCompetitorsWithDeclaredCALastYear = s.calculatePercentages(numCompetitorsWithDeclaredCALastYear, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredCA2YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredCA2YearsAgo, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredCA3YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredCA3YearsAgo, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredRevenueLastYear = s.calculatePercentages(numCompetitorsWithDeclaredRevenueLastYear, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredEmployeesLastYear = s.calculatePercentages(numCompetitorsWithDeclaredEmployeesLastYear, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredRevenue2YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredRevenue2YearsAgo, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredEmployees2YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredEmployees2YearsAgo, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredRevenue3YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredRevenue3YearsAgo, numCompetitors)
			stats.PercentageCompetitorsWithDeclaredEmployees3YearsAgo = s.calculatePercentages(numCompetitorsWithDeclaredEmployees3YearsAgo, numCompetitors)
		}

		if numCompetitorsWithDeclaredCALastYear > 0 {
			stats.CompetitorsAverageCALastYear = s.calculateAverages(stats.CAArrayLastYear)
		}
		if numCompetitorsWithDeclaredCA2YearsAgo > 0 {
			stats.CompetitorsAverageCA2YearsAgo = s.calculateAverages(stats.CAArray2YearsAgo)
		}
		if numCompetitorsWithDeclaredCA3YearsAgo > 0 {
			stats.CompetitorsAverageCA3YearsAgo = s.calculateAverages(stats.CAArray3YearsAgo)
		}

		if numCompetitorsWithDeclaredRevenueLastYear > 0 {
			stats.CompetitorsAverageRevenueLastYear = s.calculateAverages(stats.RevenueArrayLastYear)
		}
		if numCompetitorsWithDeclaredRevenue2YearsAgo > 0 {
			stats.CompetitorsAverageRevenue2YearsAgo = s.calculateAverages(stats.RevenueArray2YearsAgo)
		}
		if numCompetitorsWithDeclaredRevenue3YearsAgo > 0 {
			stats.CompetitorsAverageRevenue3YearsAgo = s.calculateAverages(stats.RevenueArray3YearsAgo)
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

	
	if (totalNumCompetitors > 0) {
		totalPercentageCompetitorsWithDeclaredCALastYear = s.calculatePercentages(totalNumCompetitorsWithDeclaredCALastYear, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredCA2YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredCA2YearsAgo, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredCA3YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredCA3YearsAgo, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredRevenueLastYear = s.calculatePercentages(totalNumCompetitorsWithDeclaredRevenueLastYear, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredEmployeesLastYear = s.calculatePercentages(totalNumCompetitorsWithDeclaredEmployeesLastYear, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredRevenue2YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredRevenue2YearsAgo, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredEmployees2YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredEmployees2YearsAgo, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredRevenue3YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredRevenue3YearsAgo, totalNumCompetitors)
		totalPercentageCompetitorsWithDeclaredEmployees3YearsAgo = s.calculatePercentages(totalNumCompetitorsWithDeclaredEmployees3YearsAgo, totalNumCompetitors)
	}

	if (totalNumCompetitorsWithDeclaredCALastYear > 0) {
		totalCompetitorsAverageCALastYear = s.calculateAverages(totalCAArrayLastYear)
	}
	if (totalNumCompetitorsWithDeclaredCA2YearsAgo > 0) {
		totalCompetitorsAverageCA2YearsAgo = s.calculateAverages(totalCAArray2YearsAgo)
	}
	if (totalNumCompetitorsWithDeclaredCA3YearsAgo > 0) {
		totalCompetitorsAverageCA3YearsAgo = s.calculateAverages(totalCAArray3YearsAgo)
	}
	if (totalNumCompetitorsWithDeclaredRevenueLastYear > 0) {
		totalCompetitorsAverageRevenueLastYear = s.calculateAverages(totalRevenueArrayLastYear)
	}
	if (totalNumCompetitorsWithDeclaredRevenue2YearsAgo > 0) {
		totalCompetitorsAverageRevenue2YearsAgo = s.calculateAverages(totalRevenueArray2YearsAgo)
	}
	if (totalNumCompetitorsWithDeclaredRevenue3YearsAgo > 0) {
		totalCompetitorsAverageRevenue3YearsAgo = s.calculateAverages(totalRevenueArray3YearsAgo)
	}
	if (totalNumCompetitorsWithDeclaredEmployeesLastYear > 0) {
		totalCompetitorsAverageEmployeesLastYear = int(math.Round(float64(totalCompetitorsAverageEmployeesLastYear) / float64(totalNumCompetitorsWithDeclaredEmployeesLastYear)))
	}
	if (totalNumCompetitorsWithDeclaredEmployees2YearsAgo > 0) {
		totalCompetitorsAverageEmployees2YearsAgo = int(math.Round(float64(totalCompetitorsAverageEmployees2YearsAgo) / float64(totalNumCompetitorsWithDeclaredEmployees2YearsAgo)))
	}
	if (totalNumCompetitorsWithDeclaredEmployees3YearsAgo > 0) {
		totalCompetitorsAverageEmployees3YearsAgo = int(math.Round(float64(totalCompetitorsAverageEmployees3YearsAgo) / float64(totalNumCompetitorsWithDeclaredEmployees3YearsAgo)))
	}

	response.Averages = models.CompetitionResponse{
		NumCompetitorsWithAStatus: totalNumCompetitorsWithAStatus,
		NumCompetitorsWithBStatus: totalNumCompetitorsWithBStatus,
		NumCompetitorsWithCStatus: totalNumCompetitorsWithCStatus,
		NumCompetitorsWithDStatus: totalNumCompetitorsWithDStatus,
		NumCompetitorsWithEStatus: totalNumCompetitorsWithEStatus,
		CompetitorsAverageCALastYear: totalCompetitorsAverageCALastYear,
		CompetitorsAverageCA2YearsAgo: totalCompetitorsAverageCA2YearsAgo,
		CompetitorsAverageCA3YearsAgo: totalCompetitorsAverageCA3YearsAgo,
		CompetitorsAverageRevenueLastYear: totalCompetitorsAverageRevenueLastYear,
		CompetitorsAverageEmployeesLastYear: totalCompetitorsAverageEmployeesLastYear,
		CompetitorsAverageRevenue2YearsAgo: totalCompetitorsAverageRevenue2YearsAgo,
		CompetitorsAverageEmployees2YearsAgo: totalCompetitorsAverageEmployees2YearsAgo,
		CompetitorsAverageRevenue3YearsAgo: totalCompetitorsAverageRevenue3YearsAgo,
		CompetitorsAverageEmployees3YearsAgo: totalCompetitorsAverageEmployees3YearsAgo,
		PercentageCompetitorsWithDeclaredCALastYear: totalPercentageCompetitorsWithDeclaredCALastYear,
		PercentageCompetitorsWithDeclaredCA2YearsAgo: totalPercentageCompetitorsWithDeclaredCA2YearsAgo,
		PercentageCompetitorsWithDeclaredCA3YearsAgo: totalPercentageCompetitorsWithDeclaredCA3YearsAgo,
		PercentageCompetitorsWithDeclaredRevenueLastYear: totalPercentageCompetitorsWithDeclaredRevenueLastYear,
		PercentageCompetitorsWithDeclaredEmployeesLastYear: totalPercentageCompetitorsWithDeclaredEmployeesLastYear,
		PercentageCompetitorsWithDeclaredRevenue2YearsAgo: totalPercentageCompetitorsWithDeclaredRevenue2YearsAgo,
		PercentageCompetitorsWithDeclaredEmployees2YearsAgo: totalPercentageCompetitorsWithDeclaredEmployees2YearsAgo,
		PercentageCompetitorsWithDeclaredRevenue3YearsAgo: totalPercentageCompetitorsWithDeclaredRevenue3YearsAgo,
		PercentageCompetitorsWithDeclaredEmployees3YearsAgo: totalPercentageCompetitorsWithDeclaredEmployees3YearsAgo,
		CAArrayLastYear: totalCAArrayLastYear,
		CAArray2YearsAgo: totalCAArray2YearsAgo,
		CAArray3YearsAgo: totalCAArray3YearsAgo,
		RevenueArrayLastYear: totalRevenueArrayLastYear,
		RevenueArray2YearsAgo: totalRevenueArray2YearsAgo,
		RevenueArray3YearsAgo: totalRevenueArray3YearsAgo,
		EmployeesArrayLastYear: totalEmployeesArrayLastYear,
		EmployeesArray2YearsAgo: totalEmployeesArray2YearsAgo,
		EmployeesArray3YearsAgo: totalEmployeesArray3YearsAgo,
		NumCompetitorsWithConsistentIncrease: totalNumCompetitorsWithConsistentIncrease,
		NumCompetitorsWithConsistentDecrease: totalNumCompetitorsWithConsistentDecrease,
		NumCompetitorsWithMixedTrend: totalNumCompetitorsWithMixedTrend,
		OldDataUsed: totalOldDataUsed,
	}

	return response, nil
}
	