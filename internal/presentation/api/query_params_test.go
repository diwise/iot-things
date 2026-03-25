package api

import (
	"net/url"
	"testing"

	app "github.com/diwise/iot-things/internal/application"
)

func TestParseThingQueryAppliesDefaultsAndNumericFilters(t *testing.T) {
	values := url.Values{
		"type":    []string{"Container"},
		"v[maxd]": []string{"12.5"},
	}

	query, err := parseThingQuery(values, []string{"default"})
	if err != nil {
		t.Fatalf("parseThingQuery returned error: %v", err)
	}

	if query.Page.Limit != app.DefaultQueryLimit || query.Page.Offset != 0 {
		t.Fatalf("unexpected default pagination: %#v", query.Page)
	}
	if len(query.Types) != 1 || query.Types[0] != "Container" {
		t.Fatalf("unexpected types: %#v", query.Types)
	}
	if len(query.NumericFilters) != 1 {
		t.Fatalf("expected one numeric filter, got %#v", query.NumericFilters)
	}
	if query.NumericFilters[0].Op != app.CompareGreaterThan {
		t.Fatalf("expected default numeric filter operator gt, got %q", query.NumericFilters[0].Op)
	}
}

func TestParseThingQueryRejectsInvalidLimit(t *testing.T) {
	_, err := parseThingQuery(url.Values{"limit": []string{"0"}}, []string{"default"})
	if err == nil {
		t.Fatal("expected invalid limit to fail")
	}
}

func TestParseValueQueryRejectsConflictingModes(t *testing.T) {
	_, err := parseValueQuery(url.Values{
		"thingid":  []string{"thing-1"},
		"latest":   []string{"true"},
		"distinct": []string{"v"},
	})
	if err == nil {
		t.Fatal("expected conflicting latest/distinct mode to fail")
	}
}

func TestParseValueQueryRejectsNegativeOffset(t *testing.T) {
	_, err := parseValueQuery(url.Values{"offset": []string{"-1"}})
	if err == nil {
		t.Fatal("expected negative offset to fail")
	}
}
