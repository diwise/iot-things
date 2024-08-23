package storage

import (
	"encoding/json"
	"fmt"
)

var ErrAlreadyExists error = fmt.Errorf("thing already exists")
var ErrNotExist error = fmt.Errorf("thing does not exists")

type thingMap map[string]any

func (t thingMap) getString(key string) (string, bool) {
	if v, ok := t[key]; ok {
		if s, ok := v.(string); ok {
			return s, true
		}
	}
	return "", false
}
func (t thingMap) ThingID() string {
	if thingId, ok := t.getString("thing_id"); ok {
		return thingId
	}
	return ""
}
func (t thingMap) ID() string {
	if id, ok := t.getString("id"); ok {
		return id
	}
	return ""
}
func (t thingMap) Type() string {
	if type_, ok := t.getString("type"); ok {
		return type_
	}
	return ""
}
func (t thingMap) Tenant() string {
	if type_, ok := t.getString("tenant"); ok {
		return type_
	}
	return ""
}
func (t thingMap) Location() (float64, float64, bool) {
	if loc, ok_ := t["location"]; ok_ {
		if loc_, ok_ := loc.(map[string]any); ok_ {
			var lat, lon float64
			var latOk, lonOk bool
			if lat_, latOk := loc_["latitude"].(float64); latOk {
				lat = lat_				
			}
			if lon_, lonOk := loc_["longitude"].(float64);lonOk {
				lon = lon_
			}
			
			return lat, lon, latOk && lonOk
		}
	}
	return 0,0,false
}
func (t thingMap) Data() string {
	b, _ := t.Bytes()
	return string(b)
}
func (t thingMap) Bytes() ([]byte, error) {
	b, err := json.Marshal(t)
	return b, err
}
