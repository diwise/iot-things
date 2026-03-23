package storage

import (
	"strings"
	"testing"

	app "github.com/diwise/iot-things/internal/application"
)

func TestNewQueryThingsParamsParameterizesRefDeviceJSONBFilter(t *testing.T) {
	query, args := newQueryThingsParams(app.WithRefDevice(`device' OR 1=1 --`))

	if !strings.Contains(query, "data->'refDevices' @> CAST(@refdevice_filter AS jsonb)") {
		t.Fatalf("expected parameterized refdevice filter, got query %q", query)
	}

	if strings.Contains(query, `device' OR 1=1 --`) {
		t.Fatalf("expected raw refdevice value to stay out of query, got query %q", query)
	}

	if got := args["refdevice_filter"]; got != `[{"deviceID":"device' OR 1=1 --"}]` {
		t.Fatalf("unexpected refdevice filter arg: %#v", got)
	}
}

func TestNewQueryValuesParamsParameterizesThingIDAndValueName(t *testing.T) {
	query, args := newQueryValuesParams(
		app.WithThingID(`thing' OR 1=1 --`),
		app.WithValueName(`temp' OR 1=1 --`),
	)

	if !strings.Contains(query, "id LIKE @thingid_pattern") {
		t.Fatalf("expected thingid pattern parameter, got query %q", query)
	}

	if !strings.Contains(query, "id LIKE @value_name_pattern") {
		t.Fatalf("expected value name pattern parameter, got query %q", query)
	}

	if strings.Contains(query, `thing' OR 1=1 --`) || strings.Contains(query, `temp' OR 1=1 --`) {
		t.Fatalf("expected raw LIKE values to stay out of query, got query %q", query)
	}

	if got := args["thingid_pattern"]; got != `thing' OR 1=1 --/%` {
		t.Fatalf("unexpected thingid pattern arg: %#v", got)
	}

	if got := args["value_name_pattern"]; got != `%/temp' OR 1=1 --` {
		t.Fatalf("unexpected value name pattern arg: %#v", got)
	}
}

func TestNewQueryThingsParamsUsesNamedParameterForDynamicJSONBField(t *testing.T) {
	query, args := newQueryThingsParams(
		app.WithFieldNameValue("maxd", []string{"12.5"}),
		app.WithOperator("gt"),
	)

	if !strings.Contains(query, "(data->>'maxd')::numeric > @field_maxd") {
		t.Fatalf("expected named parameter for dynamic field filter, got query %q", query)
	}

	if strings.Contains(query, "@12.500000") {
		t.Fatalf("expected query to avoid float-based placeholder, got query %q", query)
	}

	if got := args["field_maxd"]; got != 12.5 {
		t.Fatalf("unexpected dynamic field arg: %#v", got)
	}
}

func TestNewQueryThingsParamsRejectsUnsafeDynamicJSONBFieldName(t *testing.T) {
	query, args := newQueryThingsParams(
		app.WithFieldNameValue("maxd')::numeric > 0 OR 1=1 --", []string{"12.5"}),
		app.WithOperator("gt"),
	)

	if strings.Contains(query, "OR 1=1") {
		t.Fatalf("expected unsafe field name to stay out of query, got query %q", query)
	}

	if len(args) != 2 {
		t.Fatalf("expected only default pagination args, got %#v", args)
	}
}
