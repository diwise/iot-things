package things

import (
	"encoding/json"
	"strings"
)

// InputSpec beskriver en namngiven ingångs källsignal: vilket LwM2M-objekt
// (URN) och vilken resurs som matar den. Tabellen per typ är data och används
// för validering av bindningar, migrering från refDevices samt som brygga i
// avvaktan på att alla anrop sker via bindningar.
type InputSpec struct {
	Name     string
	Object   string
	Resource string
}

// Matches rapporterar om en mätning motsvarar ingångens objekt och resurs.
func (s InputSpec) Matches(m Measurement) bool {
	return m.Urn == s.Object && resourceOf(m.ID) == s.Resource
}

func resourceOf(id string) string {
	if i := strings.LastIndex(id, "/"); i >= 0 {
		return id[i+1:]
	}
	return id
}

// inputTable är den deklarativa mappningen typ -> ingångar. Ersätter de
// tidigare hårdkodade hasX-URN/suffix-reglerna.
var inputTable = map[string][]InputSpec{
	"building": {
		{Name: "energy", Object: EnergyURN, Resource: "5700"},
		{Name: "power", Object: PowerURN, Resource: "5700"},
		{Name: "temperature", Object: TemperatureURN, Resource: "5700"},
	},
	"container": {
		{Name: "distance", Object: DistanceURN, Resource: "5700"},
	},
	"desk": {
		{Name: "presence", Object: DigitalInputURN, Resource: "5500"},
		{Name: "presence", Object: PresenceURN, Resource: "5500"},
	},
	"lifebuoy": {
		{Name: "presence", Object: DigitalInputURN, Resource: "5500"},
		{Name: "presence", Object: PresenceURN, Resource: "5500"},
	},
	"passage": {
		{Name: "digitalInput", Object: DigitalInputURN, Resource: "5500"},
	},
	"pointofinterest": {
		{Name: "temperature", Object: TemperatureURN, Resource: "5700"},
	},
	"pumpingstation": {
		{Name: "digitalInput", Object: DigitalInputURN, Resource: "5500"},
	},
	"room": {
		{Name: "temperature", Object: TemperatureURN, Resource: "5700"},
		{Name: "humidity", Object: HumidityURN, Resource: "5700"},
		{Name: "illuminance", Object: IlluminanceURN, Resource: "5700"},
		{Name: "co2", Object: AirQualityURN, Resource: "17"},
	},
	"sewer": {
		{Name: "distance", Object: DistanceURN, Resource: "5700"},
		{Name: "digitalInput", Object: DigitalInputURN, Resource: "5500"},
	},
	"sink": {
		{Name: "temperature", Object: TemperatureURN, Resource: "5700"},
		{Name: "presence", Object: PresenceURN, Resource: "5500"},
		{Name: "power", Object: PowerURN, Resource: "5700"},
		{Name: "energy", Object: EnergyURN, Resource: "5700"},
		{Name: "distance", Object: DistanceURN, Resource: "5700"},
		{Name: "digitalInput", Object: DigitalInputURN, Resource: "5500"},
		{Name: "illuminance", Object: IlluminanceURN, Resource: "5700"},
		{Name: "humidity", Object: HumidityURN, Resource: "5700"},
	},
	"watermeter": {
		{Name: "volume", Object: WaterMeterURN, Resource: "1"},
		{Name: "leakage", Object: WaterMeterURN, Resource: "10"},
		{Name: "backflow", Object: WaterMeterURN, Resource: "11"},
		{Name: "fraud", Object: WaterMeterURN, Resource: "13"},
	},
}

// InputsFor returnerar ingångarna för en saktyp (lowercase). Tomt om typen är
// okänd.
func InputsFor(thingType string) []InputSpec {
	return inputTable[strings.ToLower(thingType)]
}

// InputExists rapporterar om en ingång är giltig för saktypen.
func InputExists(thingType, input string) bool {
	for _, s := range InputsFor(thingType) {
		if s.Name == input {
			return true
		}
	}
	return false
}

// BindingsFromRefDevices expanderar en lista av refDevices (deviceID) till
// bindningar för alla ingångar som saktypen har. Används för att bevara
// bakåtkompatibilitet på skrivvägen.
func BindingsFromRefDevices(refDevices []any, thingType string) []map[string]any {
	specs := InputsFor(thingType)
	var bindings []map[string]any
	seen := make(map[string]struct{})
	for _, r := range refDevices {
		rm, ok := r.(map[string]any)
		if !ok {
			continue
		}
		deviceID, _ := rm["deviceID"].(string)
		if deviceID == "" {
			continue
		}
		for _, in := range specs {
			key := deviceID + "|" + in.Name
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = struct{}{}
			bindings = append(bindings, map[string]any{
				"deviceID": deviceID,
				"object":   in.Object,
				"resource": in.Resource,
				"input":    in.Name,
			})
		}
	}
	return bindings
}

// NormalizeInputBindings konverterar legacy refDevices i indata till
// bindningar när inga bindningar anges. Anropas på skrivvägen (Add/Update),
// inte vid läsning av lagrad data.
func NormalizeInputBindings(b []byte) ([]byte, error) {
	var m map[string]any
	if err := json.Unmarshal(b, &m); err != nil {
		return b, err
	}
	if _, ok := m["bindings"]; ok {
		return b, nil
	}
	refs, ok := m["refDevices"].([]any)
	if !ok {
		return b, nil
	}
	thingType, _ := m["type"].(string)
	bindings := BindingsFromRefDevices(refs, thingType)
	delete(m, "refDevices")
	if len(bindings) > 0 {
		m["bindings"] = bindings
	}
	return json.Marshal(m)
}

// BindingValidForType kräver att bindningens ingång finns för typen och att
// dess objekt/resurs motsvarar ingångens källsignal. Det förhindrar att en
// bindning kopplar fel signal till en ingång.
func BindingValidForType(thingType string, b Binding) bool {
	for _, s := range InputsFor(thingType) {
		if s.Name == b.Input && s.Object == b.Object && s.Resource == b.Resource {
			return true
		}
	}
	return false
}

// resolveInput hittar den första ingången som matchar en mätning.
func resolveInput(thingType string, m Measurement) (string, bool) {
	for _, s := range InputsFor(thingType) {
		if s.Matches(m) {
			return s.Name, true
		}
	}
	return "", false
}
