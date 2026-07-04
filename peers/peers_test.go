package peers

import (
	"math"
	"testing"
	"time"

	"go.sia.tech/peerd/geoip"
	"go.uber.org/zap/zaptest"
)

type stubStore struct {
	peers     []Peer
	locations map[string][]geoip.Location
}

func (s *stubStore) AddPeer(address string) (bool, error) { return false, nil }
func (s *stubStore) AddScan(scan PeerScan) error          { return nil }
func (s *stubStore) PeersForScan(timeout time.Duration, limit int) ([]Peer, error) {
	return nil, nil
}
func (s *stubStore) Peer(address string) (Peer, error) { return Peer{}, ErrNotFound }
func (s *stubStore) Peers(offset, limit int) ([]Peer, error) {
	if offset >= len(s.peers) {
		return nil, nil
	}
	return s.peers[offset:min(offset+limit, len(s.peers))], nil
}
func (s *stubStore) PeerLocations(address string) ([]geoip.Location, error) {
	return s.locations[address], nil
}

func TestAdjustFailureRate(t *testing.T) {
	almostEqual := func(a, b float64) bool {
		return math.Abs(a-b) <= 1e-9
	}

	if got := adjustFailureRate(0, false); !almostEqual(got, 0.2) {
		t.Fatalf("expected failure to move rate from 0 to 0.2, got %v", got)
	} else if got := adjustFailureRate(0.5, true); !almostEqual(got, 0.4) {
		t.Fatalf("expected success to move rate from 0.5 to 0.4, got %v", got)
	} else if got := adjustFailureRate(1, false); !almostEqual(got, 1) {
		t.Fatalf("expected failure to keep rate at 1, got %v", got)
	}

	rate := 1.0
	for range 50 {
		next := adjustFailureRate(rate, true)
		if next >= rate {
			t.Fatalf("expected success to lower the rate, got %v -> %v", rate, next)
		}
		rate = next
	}
	if rate > 0.001 {
		t.Fatalf("expected rate to decay towards zero, got %v", rate)
	}
}

func TestValidPeerAddress(t *testing.T) {
	tests := []struct {
		addr  string
		valid bool
	}{
		{"1.2.3.4:9981", true},
		{"[2001:4860:4860::8888]:9981", true},
		{"1.2.3.4", false},          // missing port
		{"1.2.3.4:0", false},        // zero port
		{"1.2.3.4:99999", false},    // port out of range
		{"127.0.0.1:9981", false},   // loopback
		{"[::1]:9981", false},       // loopback
		{"192.168.1.1:9981", false}, // private
		{"10.0.0.1:9981", false},    // private
		{"0.0.0.0:9981", false},     // unspecified
	}
	for _, tc := range tests {
		if got := validPeerAddress(tc.addr); got != tc.valid {
			t.Fatalf("validPeerAddress(%q) = %v, expected %v", tc.addr, got, tc.valid)
		}
	}
}

func TestBootstrapPeers(t *testing.T) {
	now := time.Now()
	store := &stubStore{
		peers: []Peer{
			{Address: "reliable1:9981", FailureRate: 0.1, LastSuccessfulScan: now},
			{Address: "unreliable:9981", FailureRate: 0.9, LastSuccessfulScan: now},
			{Address: "stale:9981", FailureRate: 0.1, LastSuccessfulScan: now.Add(-48 * time.Hour)},
			{Address: "nolocation:9981", FailureRate: 0.1, LastSuccessfulScan: now},
			{Address: "tooclose:9981", FailureRate: 0.2, LastSuccessfulScan: now},
			{Address: "reliable2:9981", FailureRate: 0.2, LastSuccessfulScan: now},
		},
		locations: map[string][]geoip.Location{
			"reliable1:9981":  {{CountryCode: "BE", Latitude: 50.8503, Longitude: 4.3517}}, // Brussels
			"unreliable:9981": {{CountryCode: "US", Latitude: 40.7128, Longitude: -74.0060}},
			"stale:9981":      {{CountryCode: "DE", Latitude: 52.5200, Longitude: 13.4050}},
			"tooclose:9981":   {{CountryCode: "BE", Latitude: 51.2194, Longitude: 4.4025}}, // Antwerp, ~41 km from Brussels
			"reliable2:9981":  {{CountryCode: "FR", Latitude: 48.8566, Longitude: 2.3522}}, // Paris
		},
	}

	m := &Manager{
		store: store,
		log:   zaptest.NewLogger(t),
	}

	peers, err := m.BootstrapPeers(10)
	if err != nil {
		t.Fatal(err)
	}
	expected := []string{"reliable1:9981", "reliable2:9981"}
	if len(peers) != len(expected) {
		t.Fatalf("expected %d peers, got %d: %v", len(expected), len(peers), peers)
	}
	for i, addr := range expected {
		if peers[i] != addr {
			t.Fatalf("expected peer %d to be %q, got %q", i, addr, peers[i])
		}
	}
}
