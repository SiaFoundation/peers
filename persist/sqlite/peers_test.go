package sqlite

import (
	"errors"
	"path/filepath"
	"testing"
	"time"

	"go.sia.tech/peerd/geoip"
	"go.sia.tech/peerd/peers"
	"go.uber.org/zap/zaptest"
)

func testStore(t *testing.T) *Store {
	t.Helper()

	db, err := OpenDatabase(filepath.Join(t.TempDir(), "peerd.sqlite3"), zaptest.NewLogger(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func TestAddPeer(t *testing.T) {
	db := testStore(t)

	const addr = "1.2.3.4:9981"
	if exists, err := db.AddPeer(addr); err != nil {
		t.Fatal(err)
	} else if exists {
		t.Fatal("expected peer to be new")
	}

	if exists, err := db.AddPeer(addr); err != nil {
		t.Fatal(err)
	} else if !exists {
		t.Fatal("expected peer to already exist")
	}

	p, err := db.Peer(addr)
	if err != nil {
		t.Fatal(err)
	} else if p.Address != addr {
		t.Fatalf("expected address %q, got %q", addr, p.Address)
	} else if time.Since(p.FirstSeen) > time.Minute {
		t.Fatalf("expected first seen to be recent, got %v", p.FirstSeen)
	} else if p.SuccessfulScans != 0 {
		t.Fatalf("expected no successful scans, got %d", p.SuccessfulScans)
	}

	if _, err := db.Peer("5.6.7.8:9981"); !errors.Is(err, peers.ErrNotFound) {
		t.Fatalf("expected ErrNotFound, got %v", err)
	}

	// peers that have never been scanned successfully are excluded
	if ps, err := db.Peers(0, 10); err != nil {
		t.Fatal(err)
	} else if len(ps) != 0 {
		t.Fatalf("expected no scanned peers, got %d", len(ps))
	}
}

func TestAddScan(t *testing.T) {
	db := testStore(t)

	const addr = "1.2.3.4:9981"
	if _, err := db.AddPeer(addr); err != nil {
		t.Fatal(err)
	}

	err := db.AddScan(peers.PeerScan{
		Address:      addr,
		NextScanTime: time.Now().Add(time.Hour),
		FailureRate:  0.2,
	})
	if err != nil {
		t.Fatal(err)
	}

	p, err := db.Peer(addr)
	if err != nil {
		t.Fatal(err)
	} else if p.ConsecutiveFailures != 1 {
		t.Fatalf("expected 1 consecutive failure, got %d", p.ConsecutiveFailures)
	} else if p.FailureRate != 0.2 {
		t.Fatalf("expected failure rate 0.2, got %v", p.FailureRate)
	} else if time.Since(p.LastScanAttempt) > time.Minute {
		t.Fatalf("expected last scan attempt to be recent, got %v", p.LastScanAttempt)
	} else if p.LastSuccessfulScan.UnixMilli() != 0 {
		t.Fatalf("expected no successful scan, got %v", p.LastSuccessfulScan)
	}

	brussels := geoip.Location{CountryCode: "BE", Latitude: 50.8503, Longitude: 4.3517}
	err = db.AddScan(peers.PeerScan{
		Address:       addr,
		NextScanTime:  time.Now().Add(time.Hour),
		Successful:    true,
		FailureRate:   0.1,
		CurrentHeight: 500000,
		Locations:     []geoip.Location{brussels},
	})
	if err != nil {
		t.Fatal(err)
	}

	p, err = db.Peer(addr)
	if err != nil {
		t.Fatal(err)
	} else if p.ConsecutiveFailures != 0 {
		t.Fatalf("expected 0 consecutive failures, got %d", p.ConsecutiveFailures)
	} else if p.FailureRate != 0.1 {
		t.Fatalf("expected failure rate 0.1, got %v", p.FailureRate)
	} else if p.SuccessfulScans != 1 {
		t.Fatalf("expected 1 successful scan, got %d", p.SuccessfulScans)
	} else if time.Since(p.LastSuccessfulScan) > time.Minute {
		t.Fatalf("expected last successful scan to be recent, got %v", p.LastSuccessfulScan)
	}

	if locations, err := db.PeerLocations(addr); err != nil {
		t.Fatal(err)
	} else if len(locations) != 1 {
		t.Fatalf("expected 1 location, got %d", len(locations))
	} else if locations[0] != brussels {
		t.Fatalf("expected location %v, got %v", brussels, locations[0])
	}

	err = db.AddScan(peers.PeerScan{
		Address:      addr,
		NextScanTime: time.Now().Add(time.Hour),
		Successful:   true,
		FailureRate:  0.08,
		Locations: []geoip.Location{
			{CountryCode: "FR", Latitude: 48.8566, Longitude: 2.3522},
			{CountryCode: "US", Latitude: 40.7128, Longitude: -74.0060},
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	p, err = db.Peer(addr)
	if err != nil {
		t.Fatal(err)
	} else if p.SuccessfulScans != 2 {
		t.Fatalf("expected 2 successful scans, got %d", p.SuccessfulScans)
	}

	locations, err := db.PeerLocations(addr)
	if err != nil {
		t.Fatal(err)
	} else if len(locations) != 2 {
		t.Fatalf("expected 2 locations, got %d", len(locations))
	}
	for _, loc := range locations {
		if loc == brussels {
			t.Fatalf("expected old location to be replaced, got %v", loc)
		}
	}
}

func TestPeersForScan(t *testing.T) {
	db := testStore(t)

	addrs := []string{"1.1.1.1:9981", "2.2.2.2:9981", "3.3.3.3:9981"}
	for _, addr := range addrs {
		if _, err := db.AddPeer(addr); err != nil {
			t.Fatal(err)
		}
	}

	seen := make(map[string]bool)
	batch, err := db.PeersForScan(time.Hour, 2)
	if err != nil {
		t.Fatal(err)
	} else if len(batch) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(batch))
	}
	for _, p := range batch {
		seen[p.Address] = true
	}

	batch, err = db.PeersForScan(time.Hour, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(batch) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(batch))
	} else if seen[batch[0].Address] {
		t.Fatalf("expected a new peer, got %q again", batch[0].Address)
	}
	seen[batch[0].Address] = true

	if len(seen) != len(addrs) {
		t.Fatalf("expected %d distinct peers, got %d", len(addrs), len(seen))
	}

	if batch, err := db.PeersForScan(time.Hour, 10); err != nil {
		t.Fatal(err)
	} else if len(batch) != 0 {
		t.Fatalf("expected no peers due for scan, got %d", len(batch))
	}

	err = db.AddScan(peers.PeerScan{
		Address:      addrs[0],
		NextScanTime: time.Now().Add(-time.Minute),
		FailureRate:  0.2,
	})
	if err != nil {
		t.Fatal(err)
	}

	if batch, err := db.PeersForScan(time.Hour, 10); err != nil {
		t.Fatal(err)
	} else if len(batch) != 1 {
		t.Fatalf("expected 1 peer, got %d", len(batch))
	} else if batch[0].Address != addrs[0] {
		t.Fatalf("expected peer %q, got %q", addrs[0], batch[0].Address)
	}
}

func TestPeers(t *testing.T) {
	db := testStore(t)

	addScan := func(addr string, successful bool, failureRate float64) {
		t.Helper()
		if err := db.AddScan(peers.PeerScan{
			Address:      addr,
			NextScanTime: time.Now().Add(time.Hour),
			Successful:   successful,
			FailureRate:  failureRate,
		}); err != nil {
			t.Fatal(err)
		}
	}

	for _, addr := range []string{"a:9981", "b:9981", "c:9981", "d:9981"} {
		if _, err := db.AddPeer(addr); err != nil {
			t.Fatal(err)
		}
	}

	addScan("a:9981", true, 0.05)
	addScan("a:9981", true, 0.1)
	addScan("b:9981", true, 0.1)
	addScan("c:9981", true, 0.5)
	addScan("d:9981", false, 0.2) // never scanned successfully, excluded

	expected := []string{"a:9981", "b:9981", "c:9981"}
	ps, err := db.Peers(0, 10)
	if err != nil {
		t.Fatal(err)
	} else if len(ps) != len(expected) {
		t.Fatalf("expected %d peers, got %d", len(expected), len(ps))
	}
	for i, addr := range expected {
		if ps[i].Address != addr {
			t.Fatalf("expected peer %d to be %q, got %q", i, addr, ps[i].Address)
		}
	}

	if ps, err := db.Peers(0, 2); err != nil {
		t.Fatal(err)
	} else if len(ps) != 2 || ps[0].Address != "a:9981" || ps[1].Address != "b:9981" {
		t.Fatalf("expected [a b], got %v", ps)
	}
	if ps, err := db.Peers(2, 2); err != nil {
		t.Fatal(err)
	} else if len(ps) != 1 || ps[0].Address != "c:9981" {
		t.Fatalf("expected [c], got %v", ps)
	}
}
