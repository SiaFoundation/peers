package api

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"go.sia.tech/peerd/peers"
)

type stubPeers struct {
	peers     []peers.Peer
	bootstrap []string

	lastOffset, lastLimit int
}

func (s *stubPeers) BootstrapPeers(limit int) ([]string, error) {
	s.lastLimit = limit
	return s.bootstrap, nil
}

func (s *stubPeers) Peers(offset, limit int) ([]peers.Peer, error) {
	s.lastOffset, s.lastLimit = offset, limit
	return s.peers, nil
}

func (s *stubPeers) Peer(addr string) (peers.Peer, error) {
	for _, p := range s.peers {
		if p.Address == addr {
			return p, nil
		}
	}
	return peers.Peer{}, peers.ErrNotFound
}

func testServer(t *testing.T, stub *stubPeers) *httptest.Server {
	t.Helper()

	srv := httptest.NewServer(NewHandler(stub))
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string, v any) *http.Response {
	t.Helper()

	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if v != nil && resp.StatusCode == http.StatusOK {
		if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
			t.Fatal(err)
		}
	}
	return resp
}

func TestPeersHandler(t *testing.T) {
	stub := &stubPeers{
		peers: []peers.Peer{
			{Address: "1.2.3.4:9981", FirstSeen: time.Unix(1000, 0).UTC(), SuccessfulScans: 5},
			{Address: "5.6.7.8:9981", FirstSeen: time.Unix(2000, 0).UTC(), SuccessfulScans: 2},
		},
	}
	srv := testServer(t, stub)

	var got []peers.Peer
	if resp := get(t, srv.URL+"/peers", &got); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if len(got) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(got))
	} else if got[0] != stub.peers[0] || got[1] != stub.peers[1] {
		t.Fatalf("expected %v, got %v", stub.peers, got)
	} else if stub.lastOffset != 0 || stub.lastLimit != 100 {
		t.Fatalf("expected default offset 0 and limit 100, got %d and %d", stub.lastOffset, stub.lastLimit)
	}

	if resp := get(t, srv.URL+"/peers?offset=10&limit=5000", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if stub.lastOffset != 10 || stub.lastLimit != 1000 {
		t.Fatalf("expected offset 10 and limit 1000, got %d and %d", stub.lastOffset, stub.lastLimit)
	}

	var addrs []string
	if resp := get(t, srv.URL+"/peers?output=text", &addrs); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if len(addrs) != 2 || addrs[0] != "1.2.3.4:9981" || addrs[1] != "5.6.7.8:9981" {
		t.Fatalf("expected addresses, got %v", addrs)
	}
}

func TestPeerHandler(t *testing.T) {
	stub := &stubPeers{
		peers: []peers.Peer{
			{Address: "1.2.3.4:9981", FirstSeen: time.Unix(1000, 0).UTC(), SuccessfulScans: 5},
		},
	}
	srv := testServer(t, stub)

	var got peers.Peer
	if resp := get(t, srv.URL+"/peers/1.2.3.4:9981", &got); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if got != stub.peers[0] {
		t.Fatalf("expected %v, got %v", stub.peers[0], got)
	}

	if resp := get(t, srv.URL+"/peers/5.6.7.8:9981", nil); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected status 404, got %d", resp.StatusCode)
	}
}

func TestBootstrapHandler(t *testing.T) {
	stub := &stubPeers{
		bootstrap: []string{"1.2.3.4:9981", "5.6.7.8:9981"},
	}
	srv := testServer(t, stub)

	var got []string
	if resp := get(t, srv.URL+"/bootstrap", &got); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if len(got) != 2 || got[0] != stub.bootstrap[0] || got[1] != stub.bootstrap[1] {
		t.Fatalf("expected %v, got %v", stub.bootstrap, got)
	} else if stub.lastLimit != 50 {
		t.Fatalf("expected default limit 50, got %d", stub.lastLimit)
	}

	if resp := get(t, srv.URL+"/bootstrap?limit=5", nil); resp.StatusCode != http.StatusOK {
		t.Fatalf("expected status 200, got %d", resp.StatusCode)
	} else if stub.lastLimit != 5 {
		t.Fatalf("expected limit 5, got %d", stub.lastLimit)
	}
}
