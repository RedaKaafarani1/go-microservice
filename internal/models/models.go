package models

// Point represents a geographical point with latitude and longitude
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
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
} 