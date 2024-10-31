package types

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

type ThingUpdated struct {
	ID        string    `json:"id"`
	Type      string    `json:"type"`
	Thing     any       `json:"thing,omitempty"`
	Tenant    string    `json:"tenant"`
	Timestamp time.Time `json:"timestamp"`
}

func (t *ThingUpdated) Body() []byte {
	b, _ := json.Marshal(t)
	return b
}
func (t *ThingUpdated) ContentType() string {
	return fmt.Sprintf("application/vnd.diwise.%s+json", strings.ToLower(t.Type))
}
func (t *ThingUpdated) TopicName() string {
	return "thing.updated"
}
