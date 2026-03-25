package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"slices"
	"strings"

	app "github.com/diwise/iot-things/internal/application"
	"github.com/diwise/iot-things/internal/application/things"
)

func presentThingQueryJSON(r *http.Request, result app.QueryResult) ([]byte, error) {
	data := make([]map[string]any, 0, len(result.Data))
	for _, item := range result.Data {
		mapped, err := mapThing(item)
		if err != nil {
			return nil, err
		}
		data = append(data, mapped)
	}

	response := NewApiResponse(r, data, uint64(result.Count), uint64(result.TotalCount), uint64(result.Offset), uint64(result.Limit))
	return json.Marshal(response)
}

func presentThingDetailJSON(r *http.Request, thingData []byte, values app.QueryResult) ([]byte, error) {
	thing, err := mapThing(thingData)
	if err != nil {
		return nil, err
	}

	thing["values"] = transformValues(r, values.Data)
	response := NewApiResponse(r, thing, uint64(values.Count), uint64(values.TotalCount), uint64(values.Offset), uint64(values.Limit))
	return response.Byte(), nil
}

func presentValueQueryJSON(r *http.Request, result app.QueryResult) ([]byte, error) {
	data := transformValues(r, result.Data)
	response := NewApiResponse(r, data, uint64(result.Count), uint64(result.TotalCount), uint64(result.Offset), uint64(result.Limit))
	return json.Marshal(response)
}

func presentThingQueryCSV(result app.QueryResult) ([]byte, error) {
	var csv bytes.Buffer
	if err := exportThingQueryResultAsCSV(result, &csv); err != nil {
		return nil, err
	}
	return csv.Bytes(), nil
}

func presentValueQueryCSV(result app.QueryResult) ([]byte, error) {
	var csv bytes.Buffer
	if err := exportValuesAsCSV(result, &csv); err != nil {
		return nil, err
	}
	return csv.Bytes(), nil
}

func exportThingQueryResultAsCSV(result app.QueryResult, w io.Writer) error {
	if result.Count == 0 {
		return nil
	}

	for i, b := range result.Data {
		t, err := things.ConvToThing(b)
		if err != nil {
			return err
		}

		m := make(map[string]any)
		err = json.Unmarshal(b, &m)
		if err != nil {
			return err
		}

		if i == 0 {
			header := strings.Join([]string{"id", "type", "subType", "name", "decsription", "location", "tenant", "tags", "refDevices", "args"}, ";")
			_, err := w.Write(fmt.Appendln(nil, header))
			if err != nil {
				return err
			}
		}

		asString := func(v any) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%v", v)
		}
		asTags := func(v any) string {
			if v == nil {
				return ""
			}
			values := v.([]any)
			tags := make([]string, len(values))
			for i, tag := range values {
				tags[i] = fmt.Sprintf("%v", tag)
			}

			return strings.Join(tags, ",")
		}
		asRefDevices := func(v any) string {
			if v == nil {
				return ""
			}
			devices := v.([]any)
			refDevices := make([]string, len(devices))
			for i, device := range devices {
				d := device.(map[string]any)
				refDevices[i] = fmt.Sprintf("%v", d["deviceID"])
			}
			return strings.Join(refDevices, ",")
		}
		asArgs := func(m map[string]any) string {
			args := []string{}

			for k, v := range m {
				if slices.Contains([]string{"maxd", "maxl", "meanl", "offset", "angle"}, k) {
					args = append(args, fmt.Sprintf("'%s':%f", k, v.(float64)))
				}
				if slices.Contains([]string{"alternativeName"}, k) {
					s := v.(string)
					if s != "" {
						args = append(args, fmt.Sprintf("'%s':'%s'", k, s))
					}
				}
			}

			if len(args) > 0 {
				j := "{" + strings.Join(args, ",") + "}"
				return j
			}

			return ""
		}

		lat, lon := t.LatLon()
		values := []string{
			t.ID(),
			t.Type(),
			asString(m["subType"]),
			asString(m["name"]),
			asString(m["description"]),
			fmt.Sprintf("%f,%f", lat, lon),
			t.Tenant(),
			asTags(m["tags"]),
			asRefDevices(m["refDevices"]),
			asArgs(m),
		}

		row := strings.Join(values, ";")

		_, err = w.Write(fmt.Appendln(nil, row))
		if err != nil {
			return err
		}
	}

	return nil
}

func exportValuesAsCSV(result app.QueryResult, w io.Writer) error {
	header := strings.Join([]string{"time", "id", "urn", "v", "vb", "vs", "unit", "ref"}, ";")

	if result.Count == 0 {
		_, err := w.Write([]byte(header))
		return err
	}

	for i, b := range result.Data {
		m := make(map[string]any)
		err := json.Unmarshal(b, &m)
		if err != nil {
			return err
		}

		if i == 0 {
			_, err := w.Write(fmt.Appendln(nil, header))
			if err != nil {
				return err
			}
		}

		str := func(v any) string {
			if v == nil {
				return ""
			}
			return fmt.Sprintf("%v", v)
		}

		values := []string{
			str(m["timestamp"]),
			str(m["id"]),
			str(m["urn"]),
			str(m["v"]),
			str(m["vb"]),
			str(m["vs"]),
			str(m["unit"]),
			str(m["ref"]),
		}

		row := strings.Join(values, ";")

		_, err = w.Write(fmt.Appendln(nil, row))
		if err != nil {
			return err
		}
	}

	return nil
}

func mapThing(data []byte) (map[string]any, error) {
	m := make(map[string]any)
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	mapToOutModel(m)
	return m, nil
}

func transformValues(r *http.Request, values [][]byte) any {
	group := r.URL.Query().Get("options")

	if !slices.Contains([]string{"", "groupByID", "groupByRef"}, group) {
		group = ""
	}

	flatValues := make([]json.RawMessage, 0, len(values))
	groupedValues := map[string][]json.RawMessage{}

	for _, v := range values {
		switch group {
		case "":
			flatValues = append(flatValues, json.RawMessage(v))
		case "groupByID":
			valueID := struct {
				ID string `json:"id"`
			}{}
			err := json.Unmarshal(v, &valueID)
			if err != nil {
				continue
			}

			if _, ok := groupedValues[valueID.ID]; !ok {
				groupedValues[valueID.ID] = []json.RawMessage{}
			}

			groupedValues[valueID.ID] = append(groupedValues[valueID.ID], json.RawMessage(v))
		case "groupByRef":
			valueID := struct {
				Ref string `json:"ref"`
			}{}
			err := json.Unmarshal(v, &valueID)
			if err != nil {
				continue
			}

			if _, ok := groupedValues[valueID.Ref]; !ok {
				groupedValues[valueID.Ref] = []json.RawMessage{}
			}

			groupedValues[valueID.Ref] = append(groupedValues[valueID.Ref], json.RawMessage(v))
		}
	}

	if group == "" {
		return flatValues
	}

	return groupedValues
}
