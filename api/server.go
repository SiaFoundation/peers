package api

import (
	"errors"
	"net/http"

	"go.sia.tech/jape"
	"go.sia.tech/peerd/peers"
)

// Peers provides an interface for querying peer information.
type Peers interface {
	BootstrapPeers(limit int) ([]string, error)
	Peers(offset, limit int) ([]peers.Peer, error)
	Peer(addr string) (peers.Peer, error)
}

type server struct {
	peers Peers
}

func (s *server) handleGETPeer(jc jape.Context) {
	var addr string
	jc.DecodeParam("addr", &addr)

	peer, err := s.peers.Peer(addr)
	if errors.Is(err, peers.ErrNotFound) {
		jc.Error(err, http.StatusNotFound)
		return
	} else if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}
	jc.Encode(peer)
}

func (s *server) handleGETPeers(jc jape.Context) {
	offset, limit := 0, 100
	if jc.DecodeForm("offset", &offset) != nil {
		return
	} else if jc.DecodeForm("limit", &limit) != nil {
		return
	}

	output := "json"
	if jc.DecodeForm("output", &output) != nil {
		return
	}

	if limit > 1000 {
		limit = 1000
	}

	peers, err := s.peers.Peers(offset, limit)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	switch output {
	case "json":
		jc.Encode(peers)
	case "text":
		var result []string
		for _, p := range peers {
			result = append(result, p.Address)
		}
		jc.Encode(result)
	default:
		jc.Error(nil, http.StatusBadRequest)
	}
}

func (s *server) handleGETBootstrapPeers(jc jape.Context) {
	limit := 50
	if jc.DecodeForm("limit", &limit) != nil {
		return
	}

	peers, err := s.peers.BootstrapPeers(limit)
	if err != nil {
		jc.Error(err, http.StatusInternalServerError)
		return
	}

	jc.Encode(peers)
}

// NewHandler returns a new HTTP handler for the API.
func NewHandler(peers Peers) http.Handler {
	s := &server{
		peers: peers,
	}
	return jape.Mux(map[string]jape.Handler{
		"GET /peers":       s.handleGETPeers,
		"GET /peers/:addr": s.handleGETPeer,
		"GET /bootstrap":   s.handleGETBootstrapPeers,
	})
}
