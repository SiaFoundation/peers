package geoip

import "testing"

func TestSpacedSet(t *testing.T) {
	brussels := Location{CountryCode: "BE", Latitude: 50.8503, Longitude: 4.3517}
	antwerp := Location{CountryCode: "BE", Latitude: 51.2194, Longitude: 4.4025} // ~41 km from Brussels
	paris := Location{CountryCode: "FR", Latitude: 48.8566, Longitude: 2.3522}   // ~264 km from Brussels
	nyc := Location{CountryCode: "US", Latitude: 40.7128, Longitude: -74.0060}

	ss := NewSpacedSet(50)
	if !ss.Add([]Location{brussels}) {
		t.Fatal("expected first location to be added")
	} else if ss.Add([]Location{antwerp}) {
		t.Fatal("expected location within minimum distance to be rejected")
	} else if !ss.Add([]Location{paris}) {
		t.Fatal("expected location beyond minimum distance to be added")
	} else if ss.Add([]Location{brussels}) {
		t.Fatal("expected duplicate location to be rejected")
	}

	if ss.Add([]Location{nyc, antwerp}) {
		t.Fatal("expected batch with one too-close location to be rejected")
	} else if !ss.Add([]Location{nyc}) {
		t.Fatal("expected rejected batch to not add its valid locations")
	}
}
