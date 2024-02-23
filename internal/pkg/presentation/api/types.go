package api

import "encoding/json"

type FeatureCollection struct {
	Type     string    `json:"type"`
	Features []Feature `json:"features"`
}
type Feature struct {
	ID         string   `json:"id"`
	Type       string   `json:"type"`
	Geometry   Geometry `json:"geometry"`
	Properties map[string]any
}
type Geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type QueryResponse struct {
	Data  json.RawMessage `json:"data"`
	Links *Links          `json:"links,omitempty"`
}

type Links struct {
	Self  string  `json:"self"`
	First string  `json:"first"`
	Prev  *string `json:"prev,omitempty"`
	Last  string  `json:"last"`
	Next  *string `json:"next,omitempty"`
}

