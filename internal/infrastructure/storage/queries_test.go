package storage

import (
	"strings"
	"testing"

	app "github.com/diwise/iot-things/internal/application"
)

func TestBuildThingQuerySQLParameterizesRefDeviceJSONBFilter(t *testing.T) {
	refDeviceID := `device' OR 1=1 --`

	query, args, err := buildThingQuerySQL(app.ThingQuery{
		RefDeviceID: &refDeviceID,
		Page:        app.Pagination{Limit: 100, Offset: 0},
	})
	if err != nil {
		t.Fatalf("buildThingQuerySQL returned error: %v", err)
	}

	if !strings.Contains(query, "data->'bindings' @> CAST(@refdevice_filter AS jsonb)") {
		t.Fatalf("expected parameterized refdevice filter over bindings, got query %q", query)
	}
	if strings.Contains(query, refDeviceID) {
		t.Fatalf("expected raw refdevice value to stay out of query, got query %q", query)
	}
	if got := args["refdevice_filter"]; got != `[{"deviceID":"device' OR 1=1 --"}]` {
		t.Fatalf("unexpected refdevice filter arg: %#v", got)
	}
}

func TestBuildValueQuerySQLParameterizesThingIDAndValueName(t *testing.T) {
	thingID := `thing' OR 1=1 --`
	valueName := `temp' OR 1=1 --`

	query, args, err := buildValueQuerySQL(app.ValueQuery{
		ThingID:   &thingID,
		ValueName: &valueName,
		Page:      app.Pagination{Limit: 100, Offset: 0},
	})
	if err != nil {
		t.Fatalf("buildValueQuerySQL returned error: %v", err)
	}

	if !strings.Contains(query, "id LIKE @thingid_pattern") {
		t.Fatalf("expected thingid pattern parameter, got query %q", query)
	}
	if !strings.Contains(query, "id LIKE @value_name_pattern") {
		t.Fatalf("expected value name pattern parameter, got query %q", query)
	}
	if strings.Contains(query, thingID) || strings.Contains(query, valueName) {
		t.Fatalf("expected raw LIKE values to stay out of query, got query %q", query)
	}
	if got := args["thingid_pattern"]; got != `thing' OR 1=1 --/%` {
		t.Fatalf("unexpected thingid pattern arg: %#v", got)
	}
	if got := args["value_name_pattern"]; got != `%/temp' OR 1=1 --` {
		t.Fatalf("unexpected value name pattern arg: %#v", got)
	}
}

// Fynd: tenantägarskapet för värden måste matchas bokstavligt. LIKE tolkar
// "%" och "_" i sakens id som jokertecken och kan då matcha en annan sak.
func TestAddValueTenantFilterUsesLiteralPrefixMatch(t *testing.T) {
	b := newSQLBuilder()
	addValueTenantFilter(b, []string{"default"})

	where := b.WhereClause()
	if !strings.Contains(where, "starts_with(things_values.id, t.id || '/')") {
		t.Fatalf("expected literal prefix match, got %q", where)
	}
	if strings.Contains(where, "LIKE") {
		t.Fatalf("expected no LIKE in tenant ownership filter, got %q", where)
	}
	if got := b.args["tenants"]; got == nil {
		t.Fatalf("expected tenants argument to be bound")
	}
}

// T7.5: refdevice-filtret för värden matchar enhetsdelen av ref (källans
// fullständiga recordnamn), inte hela strängen.
func TestBuildValueQuerySQLRefDeviceMatchesDevicePart(t *testing.T) {
	refDevice := "milesight:214"

	query, args, err := buildValueQuerySQL(app.ValueQuery{
		RefDeviceID: &refDevice,
		Page:        app.Pagination{Limit: 100, Offset: 0},
	})
	if err != nil {
		t.Fatalf("buildValueQuerySQL returned error: %v", err)
	}
	if !strings.Contains(query, "split_part(ref, '/', 1) = @ref") {
		t.Fatalf("expected device-part match for refdevice, got query %q", query)
	}
	if got := args["ref"]; got != refDevice {
		t.Fatalf("unexpected ref arg: %#v", got)
	}
}

func TestBuildThingQuerySQLUsesNamedParameterForDynamicJSONField(t *testing.T) {
	query, args, err := buildThingQuerySQL(app.ThingQuery{
		NumericFilters: []app.NumericFieldFilter{{
			Field: "maxd",
			Op:    app.CompareGreaterThan,
			Value: 12.5,
		}},
		Page: app.Pagination{Limit: 100, Offset: 0},
	})
	if err != nil {
		t.Fatalf("buildThingQuerySQL returned error: %v", err)
	}

	if !strings.Contains(query, "(data->>'maxd')::numeric > @thing_field_0") {
		t.Fatalf("expected named parameter for dynamic field filter, got query %q", query)
	}
	if got := args["thing_field_0"]; got != 12.5 {
		t.Fatalf("unexpected dynamic field arg: %#v", got)
	}
}

func TestBuildThingQuerySQLRejectsUnsupportedDynamicJSONField(t *testing.T) {
	_, _, err := buildThingQuerySQL(app.ThingQuery{
		NumericFilters: []app.NumericFieldFilter{{
			Field: "maxd')::numeric > 0 OR 1=1 --",
			Op:    app.CompareGreaterThan,
			Value: 12.5,
		}},
		Page: app.Pagination{Limit: 100, Offset: 0},
	})
	if err == nil {
		t.Fatal("expected unsupported field filter to return an error")
	}
}
