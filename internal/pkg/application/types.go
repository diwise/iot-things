package application

import "time"

type Thing struct {
	ThingID      string        `json:"thing_id"`
	Id           string        `json:"id"`
	Type         string        `json:"type"`
	Location     Location      `json:"location"`
	Tenant       string        `json:"tenant"`
	Measurements []Measurement `json:"measurements,omitempty"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

func NewMeasurement(ts time.Time, id, urn string) Measurement {
	return Measurement{
		//DeviceID:  deviceID,
		ID: id,
		//Name:      name,
		//Tenant:    tenant,
		Timestamp: ts,
		Urn:       urn,
	}
}

type Measurement struct {
	//DeviceID    string    `json:"deviceID"`
	ID string `json:"id"`
	//Name        string    `json:"name"`
	//Tenant      string    `json:"tenant"`
	Timestamp time.Time `json:"timestamp"`
	Urn       string    `json:"urn"`
	BoolValue *bool     `json:"vb,omitempty"`
	//Lat         float64   `json:"lat"`
	//Lon         float64   `json:"lon"`
	StringValue string   `json:"vs,omitempty"`
	Unit        string   `json:"unit,omitempty"`
	Value       *float64 `json:"v,omitempty"`
}

type QueryResult struct {
	Things     []byte
	Count      int
	Limit      int
	Offset     int
	Number     *int
	Size       *int
	TotalCount int64
}
