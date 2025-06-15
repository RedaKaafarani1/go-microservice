package models

import (
	"encoding/json"
	"fmt"
	"math"

	geom2 "github.com/peterstace/simplefeatures/geom"
	"github.com/twpayne/go-geom"
)

// Point represents a geographical point with latitude and longitude
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

// ToGeomPoint converts our Point to a go-geom Point
func (p Point) ToGeomPoint() *geom.Point {
	return geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{p.Lng, p.Lat})
}

// FromGeomPoint creates a Point from a go-geom Point
func FromGeomPoint(gp *geom.Point) Point {
	coords := gp.Coords()
	return Point{
		Lat: coords[1],
		Lng: coords[0],
	}
}

// SearchRequest represents the search criteria
type SearchRequest struct {
	NAFCodes []string `json:"nafCodes"`
	Type    string `json:"type"`
	// For FeatureCollection format
	Features []struct {
		Type       string `json:"type"`
		Properties struct{} `json:"properties"`
		Geometry   struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
	// For Feature format
	Geometry struct {
		Type        string        `json:"type"`
		Coordinates [][][]float64 `json:"coordinates"`
	} `json:"geometry"`
}

// Business represents a business entity
type Business struct {
	Name      string  `json:"name"`
	Siret     string  `json:"siret"`
	NAFCode   string  `json:"nafCode"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
	Address   string  `json:"address"`
	// Add spatial index fields
	geomPoint *geom.Point
}

// ToGeomPoint converts the business location to a go-geom Point
func (b *Business) ToGeomPoint() *geom.Point {
	if b.geomPoint == nil {
		b.geomPoint = geom.NewPoint(geom.XY).MustSetCoords(geom.Coord{b.Longitude, b.Latitude})
	}
	return b.geomPoint
}

// SpatialIndex represents a spatial index for efficient point-in-polygon queries
type SpatialIndex struct {
	businesses []*Business
	bounds     *geom.Bounds
}

// NewSpatialIndex creates a new spatial index from a list of businesses
func NewSpatialIndex(businesses []*Business) *SpatialIndex {
	if len(businesses) == 0 {
		return &SpatialIndex{
			businesses: make([]*Business, 0),
			bounds:     geom.NewBounds(geom.XY),
		}
	}

	// Pre-allocate bounds
	bounds := geom.NewBounds(geom.XY)
	minX, minY := math.MaxFloat64, math.MaxFloat64
	maxX, maxY := -math.MaxFloat64, -math.MaxFloat64

	// Calculate bounds in a single pass
	for _, business := range businesses {
		if business.Longitude < minX {
			minX = business.Longitude
		}
		if business.Longitude > maxX {
			maxX = business.Longitude
		}
		if business.Latitude < minY {
			minY = business.Latitude
		}
		if business.Latitude > maxY {
			maxY = business.Latitude
		}
	}

	bounds.Set(minX, minY, maxX, maxY)

	return &SpatialIndex{
		businesses: businesses,
		bounds:     bounds,
	}
}

// Query returns all businesses that are within the given geometry
func (s *SpatialIndex) Query(geometry geom2.Geometry) []*Business {
	if len(s.businesses) == 0 {
		return nil
	}

	// Pre-allocate result slice with reasonable capacity
	results := make([]*Business, 0, len(s.businesses)/4)

	// Check each business
	for _, business := range s.businesses {
		wkt := fmt.Sprintf("POINT(%f %f)", business.Longitude, business.Latitude)
		point, err := geom2.UnmarshalWKT(wkt)
		if err != nil {
			continue
		}
		contains, err := geom2.Contains(geometry, point)
		if err != nil {
			continue
		}
		if contains {
			results = append(results, business)
		}
	}

	return results
}

// IrisData represents the demographic data for an IRIS zone
type IrisData struct {
	// Raw data storage for all 119 keys
	RawData map[string]float64 `json:"raw_data"`

	// Basic identifiers
	IRIS     string  `json:"iris"`
	COM      string  `json:"com"`
	TYP_IRIS string  `json:"typ_iris"`
	LAB_IRIS string  `json:"lab_iris"`

	// Population data
	TotalPopulation float64            `json:"total_population"`
	PopulationByAge map[string]float64 `json:"population_by_age"`

	// Gender-specific population
	MalePopulation   float64            `json:"male_population"`
	MaleByAge        map[string]float64 `json:"male_by_age"`
	FemalePopulation float64            `json:"female_population"`
	FemaleByAge      map[string]float64 `json:"female_by_age"`

	// Professional categories
	Professionals      map[string]float64 `json:"professionals"`
	MaleProfessionals  map[string]float64 `json:"male_professionals"`
	FemaleProfessionals map[string]float64 `json:"female_professionals"`

	// Nationality
	FrenchPopulation    float64 `json:"french_population"`
	ForeignPopulation   float64 `json:"foreign_population"`
	ImmigrantPopulation float64 `json:"immigrant_population"`

	// Households
	NumberOfHouseholds   float64 `json:"number_of_households"`
	CollectiveDwellings float64 `json:"collective_dwellings"`

	// Geographic data
	Polygon *geom2.Geometry `json:"polygon"`
	Area    float64       `json:"area"`

	// Family data
	FamilyTypes map[string]float64 `json:"family_types"`

	// Household types
	HouseholdTypes map[string]float64 `json:"household_types"`

	// Activity
	ActivePopulation float64 `json:"active_population"`
	StudentPopulation float64 `json:"student_population"`

	// Housing
	TotalDwellings      float64 `json:"total_dwellings"`
	MainResidences      float64 `json:"main_residences"`
	SecondaryResidences float64 `json:"secondary_residences"`
	VacantDwellings     float64 `json:"vacant_dwellings"`
	Houses              float64 `json:"houses"`
	Apartments          float64 `json:"apartments"`

	// Dwellings by rooms
	DwellingsByRooms map[string]float64 `json:"dwellings_by_rooms"`

	// Housing construction periods
	HousingPeriods map[string]float64 `json:"housing_periods"`

	// Mobility
	Mobility map[string]float64 `json:"mobility"`

	// Housing tenure
	HousingTenure map[string]float64 `json:"housing_tenure"`

	// Vehicles
	HouseholdsByCars map[string]float64 `json:"households_by_cars"`
}

// QPData represents data about a Quartier Prioritaire
type QPData struct {
	ID string `json:"id"`
	CodeQP string  `json:"codeqp"`
	LibQP string  `json:"name"`
	Commune string `json:"commune"`
	IntersectionPercentage float64 `json:"percentage"`
}

// CommuneData represents data from the commune CSV file
type CommuneData struct {
	ID           string  `json:"id"`
	CommuneCode  string  `json:"code_insee"`
	CommuneName  string  `json:"name"`
	PostalCode   string  `json:"postal_code"`
	Percentage   float64 `json:"percentage"`
	Population   float64 `json:"-"`
	SurfaceArea  float64 `json:"-"`
	Polygon *geom2.Geometry `json:"-"`
	AverageIncome float64 `json:"-"`
}

type PostalCodeData struct {
	Percentage float64 `json:"percentage"`
	PostalCode string `json:"postal_code"`
}

// AdministrativeData groups administrative-related data
type AdministrativeData struct {
	Communes     []CommuneData     `json:"communes"`
	PostalCodes  []PostalCodeData  `json:"postal_codes"`
	SpecialZones []QPData         `json:"special_zones"`
}

// CriminalityData represents the criminality statistics for a specific crime type
type CriminalityData struct {
	IsTotal                      bool    `json:"is_total"`
	CrimesTotal                  float64 `json:"crimes_total"`
	PercentageCoveredCrimes      float64 `json:"percentage_covered_crimes"`
	PercentageRelativeToDepartmental float64 `json:"percentage_relative_to_departmental"`
	CoveredArea 				 float64 `json:"-"`
	PartialCoveredArea			 float64 `json:"-"`
	CoveredResidence			 float64 `json:"-"`
}

// CriminalityResponse represents the complete criminality statistics response
type CriminalityResponse struct {
	DrugUsage                    *CriminalityData `json:"drug_usage"`
	VehicleTheft                 *CriminalityData `json:"vehicle_theft"`
	ArmedRobberies               *CriminalityData `json:"armed_robberies"`
	HomeBurglaries               *CriminalityData `json:"home_burglaries"`
	SexualViolence               *CriminalityData `json:"sexual_violence"`
	DrugTrafficking              *CriminalityData `json:"drug_trafficking"`
	VoluntaryInjuries            *CriminalityData `json:"voluntary_injuries"`
	TheftFromVehicles            *CriminalityData `json:"theft_from_vehicles"`
	OtherVoluntaryInjuries       *CriminalityData `json:"other_voluntary_injuries"`
	TheftOfVehicleAccessories    *CriminalityData `json:"theft_of_vehicle_accessories"`
	IntrafamilyVoluntaryInjuries *CriminalityData `json:"intrafamily_voluntary_injuries"`
	VoluntaryDamageAndVandalism  *CriminalityData `json:"voluntary_damage_and_vandalism"`
	ViolentRobberiesWithoutWeapon *CriminalityData `json:"violent_robberies_without_weapon"`
	RobberiesWithoutViolenceAgainstPersons *CriminalityData `json:"robberies_without_violence_against_persons"`
}

// MedianIncome represents median income statistics
type MedianIncome struct {
	AverageIncome        float64 `json:"average_income"`
	IsFullyCovered      bool    `json:"is_fully_covered"`
	PercentageAreaCovered float64 `json:"percentage_area_covered"`
}

// Statistics represents the statistics data that can contain nested objects
type Statistics struct {
	MedianIncome MedianIncome            `json:"median_income"`
	OtherData    map[string]float64      `json:"-"`
}

// MarshalJSON implements custom JSON marshaling for Statistics
func (s Statistics) MarshalJSON() ([]byte, error) {
	// Create a map that will hold all statistics
	stats := make(map[string]interface{})
	
	// Add median income
	stats["median_income"] = s.MedianIncome
	
	// Add all other statistics, rounding to integers
	for k, v := range s.OtherData {
		stats[k] = int(math.Round(v))
	}
	
	return json.Marshal(stats)
}

// IrisResponse represents the response for the IRIS data endpoint
type IrisResponse struct {
	TotalPopulation float64            `json:"totalPopulation"`
	Data           Statistics         `json:"statistics"`
	Criminality    CriminalityResponse `json:"criminality"`
	Administrative AdministrativeData `json:"administrative"`
}

// IrisRequest represents the request for the IRIS data endpoint
type IrisRequest struct {
	Type    string `json:"type"`
	// For FeatureCollection format
	Features []struct {
		Type       string `json:"type"`
		Properties struct{} `json:"properties"`
		Geometry   struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
	// For Feature format
	Geometry struct {
		Type        string        `json:"type"`
		Coordinates [][][]float64 `json:"coordinates"`
	} `json:"geometry"`
} 

// competitor count response
type CompetitorCountResponse struct {
	NumberOfCompetitors int `json:"number_of_competitors"`
}

// CompetitionResponse represents the competition data response
type CompetitionResponse struct {
	NumCompetitorsWithAStatus int `json:"num_competitors_with_0_to_32k_revenue"`
	NumCompetitorsWithBStatus int `json:"num_competitors_with_32k_to_82k_revenue"`
	NumCompetitorsWithCStatus int `json:"num_competitors_with_82k_to_250k_revenue"`
	NumCompetitorsWithDStatus int `json:"num_competitors_with_250k_to_1m_revenue"`
	NumCompetitorsWithEStatus int `json:"num_competitors_with_1m_plus_revenue"`
	CompetitorsAverageCALastYear float64 `json:"competitors_average_revenue_last_year"`
	CompetitorsAverageCA2YearsAgo float64 `json:"competitors_average_revenue_2_years_ago"`
	CompetitorsAverageCA3YearsAgo float64 `json:"competitors_average_revenue_3_years_ago"`
	CompetitorsAverageRevenueLastYear float64 `json:"competitors_average_profits_last_year"`
	CompetitorsAverageEmployeesLastYear int `json:"competitors_average_employees_last_year"`
	CompetitorsAverageRevenue2YearsAgo float64 `json:"competitors_average_profits_2_years_ago"`
	CompetitorsAverageEmployees2YearsAgo int `json:"competitors_average_employees_2_years_ago"`
	CompetitorsAverageRevenue3YearsAgo float64 `json:"competitors_average_profits_3_years_ago"`
	CompetitorsAverageEmployees3YearsAgo int `json:"competitors_average_employees_3_years_ago"`
	PercentageCompetitorsWithDeclaredCALastYear float64 `json:"percentage_competitors_with_declared_revenue_last_year"`
	PercentageCompetitorsWithDeclaredCA2YearsAgo float64 `json:"percentage_competitors_with_declared_revenue_2_years_ago"`
	PercentageCompetitorsWithDeclaredCA3YearsAgo float64 `json:"percentage_competitors_with_declared_revenue_3_years_ago"`
	PercentageCompetitorsWithDeclaredRevenueLastYear float64 `json:"percentage_competitors_with_declared_profits_last_year"`
	PercentageCompetitorsWithDeclaredEmployeesLastYear float64 `json:"percentage_competitors_with_declared_employees_last_year"`
	PercentageCompetitorsWithDeclaredRevenue2YearsAgo float64 `json:"percentage_competitors_with_declared_profits_2_years_ago"`
	PercentageCompetitorsWithDeclaredEmployees2YearsAgo float64 `json:"percentage_competitors_with_declared_employees_2_years_ago"`
	PercentageCompetitorsWithDeclaredRevenue3YearsAgo float64 `json:"percentage_competitors_with_declared_profits_3_years_ago"`
	PercentageCompetitorsWithDeclaredEmployees3YearsAgo float64 `json:"percentage_competitors_with_declared_employees_3_years_ago"`
	CAArrayLastYear []float64 `json:"revenue_array_last_year"`
	CAArray2YearsAgo []float64 `json:"revenue_array_2_years_ago"`
	CAArray3YearsAgo []float64 `json:"revenue_array_3_years_ago"`
	RevenueArrayLastYear []float64 `json:"profits_array_last_year"`
	RevenueArray2YearsAgo []float64 `json:"profits_array_2_years_ago"`
	RevenueArray3YearsAgo []float64 `json:"profits_array_3_years_ago"`
	EmployeesArrayLastYear []float64 `json:"employees_array_last_year"`
	EmployeesArray2YearsAgo []float64 `json:"employees_array_2_years_ago"`
	EmployeesArray3YearsAgo []float64 `json:"employees_array_3_years_ago"`
	NumCompetitorsWithConsistentIncrease float64 `json:"num_competitors_with_consistent_revenue_increase"`
	NumCompetitorsWithConsistentDecrease float64 `json:"num_competitors_with_consistent_revenue_decrease"`
	NumCompetitorsWithMixedTrend float64 `json:"num_competitors_with_mixed_revenue_trend"`
	OldDataUsed bool `json:"old_data_used"`
}

// NAFCodeResponse represents the response for a specific NAF code
type NAFCodeResponse struct {
	NAFCode           string     `json:"naf_code"`
	NumberOfBusinesses int       `json:"number_of_businesses"`
	Businesses        []*Business `json:"businesses"`
}

// SearchResponse represents the response for the search endpoint
type SearchResponse struct {
	NAFCodes []NAFCodeResponse `json:"naf_codes"`
}

// CompetitorWithData represents a competitor with its basic info and competition data
type CompetitorsData struct {
	Name      string  `json:"name"`
	Siret     string  `json:"siret"`
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// NAFCodeCompetitionResponse represents the competition data for a specific NAF code
type NAFCodeCompetitionResponse struct {
	NAFCode           string     `json:"naf_code"`
	NumberOfCompetitors int       `json:"number_of_competitors"`
	Competitors       []CompetitorsData `json:"competitors"`
	CompetitionStats  CompetitionResponse  `json:"competition_stats"`
}

// CompetitionResponseByNAF represents the response for the competition data endpoint grouped by NAF code
type CompetitionResponseByNAF struct {
	NAFCodes []NAFCodeCompetitionResponse `json:"naf_codes"`
	Averages CompetitionResponse `json:"averages"`
}

// BusinessData represents the competition data for a business
type BusinessData struct {
	Name               string
	Siren             string
	NIC               string
	LegalStatus       string
	CodeAPE           string
	LabelAPE          string
	Address           string
	PostalCode        string
	City              string
	NumDepartment     string
	Department        string
	Region            string
	CodeGreffe        string
	Greffe            string
	RegistrationDate  string
	DeregistrationDate string
	Status            string
	Latitude          float64
	Longitude         float64
	PublicationDate   string
	Millesime1        string
	DateCloseEx1      string
	DurationEx1       string
	CA1               string
	Result1           string
	Employees1        string
	Millesime2        string
	DateCloseEx2      string
	DurationEx2       string
	CA2               string
	Result2           string
	Employees2        string
	Millesime3        string
	DateCloseEx3      string
	DurationEx3       string
	CA3               string
	Result3           string
	Employees3        string
	RangeCA1          string
	RangeCA2          string
	RangeCA3          string
}