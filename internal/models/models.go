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