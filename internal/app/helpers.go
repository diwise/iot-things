package app

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/diwise/senml"
	"github.com/diwise/service-chassis/pkg/infrastructure/o11y/logging"
)

func isNotZero(value float64) bool {
	return (math.Abs(value) >= 0.001)
}

func packToMeasurements(ctx context.Context, pack senml.Pack) ([]Measurement, error) {
	log := logging.GetFromContext(ctx)

	header, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return nil, fmt.Errorf("could not find header record (0)")
	}

	measurements := make([]Measurement, 0)

	urn := header.StringValue

	var errs []error

	for _, r := range pack {
		n, err := strconv.Atoi(r.Name)
		if err != nil || n == 0 {
			continue
		}

		rec, ok := pack.GetRecord(senml.FindByName(r.Name))
		if !ok {
			log.Error("could not find record", "name", r.Name)
			continue
		}

		if rec.Value == nil && rec.BoolValue == nil {
			continue
		}

		id := rec.Name
		ts, _ := rec.GetTime()

		measurement := Measurement{
			ID:        id,
			Timestamp: ts,
			Urn:       urn,
		}
		var vs *string
		if rec.StringValue != "" {
			vs = &rec.StringValue
		}
		measurement.BoolValue = rec.BoolValue
		measurement.Value = rec.Value
		measurement.StringValue = vs
		measurement.Unit = rec.Unit

		measurements = append(measurements, measurement)
	}

	return measurements, errors.Join(errs...)
}

func getDeviceID(pack senml.Pack) (string, bool) {
	r, ok := pack.GetRecord(senml.FindByName("0"))
	if !ok {
		return "", false
	}
	return strings.Split(r.Name, "/")[0], true
}

func fillingLevel(id string, distance, maxDistance, maxLevel, offset, angle float64, ts time.Time) (level Measurement, percent Measurement) {
	distance += offset

	var l, p float64
	// Calculate the current level using the configured angle (if any) and round to two decimals
	l = math.Round((maxDistance-distance)*angle*100) / 100.0

	level = Measurement{
		ID:        fmt.Sprintf("%s/%s/%s", id, "3435", "3"),
		Urn:       "urn:oma:lwm2m:ext:3435",
		Value:     &l,
		Unit:      "m",
		Timestamp: ts,
	}

	if isNotZero(maxLevel) {
		p = math.Min((distance*100.0)/maxLevel, 100.0)
		if p < 0 {
			p = 0
		} else if p > 100 {
			p = 100
		}
	} else {
		return
	}

	percent = Measurement{
		ID:        fmt.Sprintf("%s/%s/%s", id, "3435", "2"),
		Urn:       "urn:oma:lwm2m:ext:3435",
		Value:     &p,
		Unit:      "%",
		Timestamp: ts,
	}

	return
}
