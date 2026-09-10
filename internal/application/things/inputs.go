package things

import "strings"

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
