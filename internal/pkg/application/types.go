package application

type Thing struct {
	Id       string   `json:"id"`
	Type     string   `json:"type"`
	Location Location `json:"location"`
	Tenant   string   `json:"tenant"`
}

type Location struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
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
