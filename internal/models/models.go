package models

import (
	"math"

	"github.com/twpayne/go-geom"
	"github.com/twpayne/go-geom/xy"
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
	NAFCode string `json:"nafCode"`
	Type    string `json:"type"`
	Features []struct {
		Type       string `json:"type"`
		Properties struct{} `json:"properties"`
		Geometry   struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
}

// Business represents a business entity
type Business struct {
	Name      string  `json:"name"`
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

// Query returns all businesses that are within the given polygon
func (s *SpatialIndex) Query(polygon *geom.Polygon) []*Business {
	if len(s.businesses) == 0 {
		return nil
	}

	// Get polygon bounds for quick filtering
	polyBounds := polygon.Bounds()
	if polyBounds == nil {
		return nil
	}

	// Quick bounds check
	if !s.bounds.Overlaps(geom.XY, polyBounds) {
		return nil
	}

	// Pre-allocate result slice with reasonable capacity
	results := make([]*Business, 0, len(s.businesses)/4)

	// Create a point for reuse
	point := geom.NewPoint(geom.XY)

	// Check each business
	for _, business := range s.businesses {
		// Quick bounds check for each point
		if business.Longitude < polyBounds.Min(0) || business.Longitude > polyBounds.Max(0) ||
			business.Latitude < polyBounds.Min(1) || business.Latitude > polyBounds.Max(1) {
			continue
		}

		// Set point coordinates
		point.MustSetCoords(geom.Coord{business.Longitude, business.Latitude})

		// Check if point is within polygon using IsPointInRing
		ringCoords := make([]float64, 0, len(polygon.Coords()[0])*2)
		for _, coord := range polygon.Coords()[0] {
			ringCoords = append(ringCoords, coord[0], coord[1])
		}
		
		if xy.IsPointInRing(geom.XY, point.Coords(), ringCoords) {
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
	Polygon *geom.Polygon `json:"polygon"`
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
	LibQP string  `json:"qp"`
	Commune string `json:"com"`
	IntersectionPercentage float64 `json:"inter_per"`
}

// IrisResponse represents the response for the IRIS data endpoint
type IrisResponse struct {
	TotalArea       float64            `json:"totalArea"`
	TotalPopulation float64            `json:"totalPopulation"`
	Data           map[string]float64 `json:"data"`
	QPData         []QPData          `json:"qp_data"`
}

// IrisRequest represents the request for the IRIS data endpoint
type IrisRequest struct {
	Type    string `json:"type"`
	Features []struct {
		Type       string `json:"type"`
		Properties struct{} `json:"properties"`
		Geometry   struct {
			Type        string        `json:"type"`
			Coordinates [][][]float64 `json:"coordinates"`
		} `json:"geometry"`
	} `json:"features"`
} 