package peers

import (
	"context"
	"fmt"
	"math"
	"net"
	"runtime"
	"strconv"
	"sync"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/threadgroup"
	"go.sia.tech/peerd/geoip"
	"go.uber.org/zap"
)

type (
	// A PeerScan represents the result of scanning a peer.
	PeerScan struct {
		Address       string
		NextScanTime  time.Time
		Successful    bool
		FailureRate   float64
		CurrentHeight uint64

		Locations []geoip.Location
	}

	// A Peer represents a peer node in the network.
	Peer struct {
		Address             string    `json:"address"`
		FirstSeen           time.Time `json:"firstSeen"`
		LastScanAttempt     time.Time `json:"lastScanAttempt"`
		LastSuccessfulScan  time.Time `json:"lastSuccessfulScan"`
		ConsecutiveFailures int       `json:"consecutiveFailures"`
		FailureRate         float64   `json:"failureRate"`
	}

	// A Store provides an interface for persisting and retrieving peer information.
	Store interface {
		AddPeer(address string) (exists bool, err error)
		AddScan(scan PeerScan) error
		PeersForScan(limit int) ([]Peer, error)

		Peers(offset, limit int) ([]Peer, error)
		PeerLocations(address string) ([]geoip.Location, error)
	}

	// An Explorer provides an interface for querying the blockchain state.
	Explorer interface {
		ConsensusTip() (types.ChainIndex, error)
	}

	// A Manager manages peer discovery and scanning.
	Manager struct {
		tg       *threadgroup.ThreadGroup
		store    Store
		explorer Explorer
		locator  geoip.Locator
		log      *zap.Logger

		genesisState consensus.State
		genesisID    types.BlockID

		scanThreads  int
		scanInterval time.Duration
	}
)

func validPeerAddress(address string) bool {
	host, portStr, err := net.SplitHostPort(address)
	if err != nil {
		return false
	} else if port, err := strconv.ParseUint(portStr, 10, 32); err != nil || port == 0 || port > 65535 {
		return false
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return false
	}
	for _, ip := range ips {
		if ip == nil || ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() {
			return false
		}
	}
	return true
}

func (m *Manager) scanPeer(ctx context.Context, scan *PeerScan, minHeight uint64, log *zap.Logger) {
	err := withGatewayTransport(ctx, scan.Address, m.genesisID, func(t *gateway.Transport) error {
		// nodes that can return headers for the first 10 blocks are likely
		// full nodes
		_, rem, err := getHeaders(t, m.genesisState, 10, 15*time.Second)
		if err != nil {
			return fmt.Errorf("failed to get headers: %w", err)
		}
		scan.CurrentHeight = rem + 10 // approximate the current height based on the number of headers remaining
		if scan.CurrentHeight < minHeight {
			return fmt.Errorf("peer height %d is below minimum acceptable height %d", scan.CurrentHeight, minHeight)
		}
		log.Debug("headers retrieved", zap.Uint64("currentHeight", scan.CurrentHeight))

		// try to discover new peers
		for range 5 {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			sharedPeers, err := sharePeers(t, 10*time.Second)
			if err != nil || len(sharedPeers) == 0 {
				break // not critical, move on
			}
			for _, p := range sharedPeers {
				if !validPeerAddress(p) {
					continue
				}

				if exists, err := m.store.AddPeer(p); err != nil {
					log.Panic("failed to add shared peer", zap.String("sharedPeer", p), zap.Error(err))
				} else if !exists {
					log.Info("discovered new peer", zap.String("discovered", p))
				}
			}
		}

		return nil
	})
	if err != nil {
		log.Warn("peer scan failed", zap.Error(err))
		return
	}

	host, _, err := net.SplitHostPort(scan.Address)
	if err != nil {
		log.Warn("failed to split host and port", zap.Error(err))
		return
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		log.Warn("failed to resolve peer IP for geoip lookup", zap.Error(err))
		return
	}
	for _, ip := range ips {
		if ip == nil {
			continue
		}
		loc, err := m.locator.Locate(ip)
		if err != nil {
			log.Warn("failed to locate peer IP", zap.String("ip", ip.String()), zap.Error(err))
			continue
		}
		scan.Locations = append(scan.Locations, loc)
	}

	if len(scan.Locations) == 0 {
		log.Warn("no geoip locations found for peer")
		return
	}
	scan.Successful = true
	log.Info("peer scan successful")
}

// scanPeers scans all peers that need to be scanned.
func (m *Manager) scanPeers(ctx context.Context) error {
	log := m.log.Named("scanner")

	var wg sync.WaitGroup
	sema := make(chan struct{}, m.scanThreads)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		peers, err := m.store.PeersForScan(m.scanThreads)
		if err != nil {
			return fmt.Errorf("failed to retrieve peers for scanning: %w", err)
		} else if len(peers) == 0 {
			break
		}

		tip, err := m.explorer.ConsensusTip()
		if err != nil {
			return fmt.Errorf("failed to retrieve consensus tip: %w", err)
		}
		log := log.With(zap.Stringer("tip", tip))
		log.Debug("consensus tip retrieved")

		var minHeight uint64
		minBlocks := uint64((24*time.Hour)/m.genesisState.Network.BlockInterval) * 3
		if tip.Height >= minBlocks {
			minHeight = tip.Height - minBlocks
		}

		for _, p := range peers {
			sema <- struct{}{}

			log := log.With(zap.String("peer", p.Address))
			wg.Go(func() {
				defer func() { <-sema }()

				scan := PeerScan{
					Address:     p.Address,
					FailureRate: p.FailureRate,
				}
				log.Debug("starting peer scan")
				m.scanPeer(ctx, &scan, minHeight, log)
				if scan.Successful {
					scan.NextScanTime = time.Now().Add(m.scanInterval)
				} else {
					failureAdjustment := min(time.Duration(math.Pow(2, min(float64(p.ConsecutiveFailures), 16)))*time.Minute, 30*24*time.Hour)
					scan.NextScanTime = time.Now().Add(m.scanInterval + failureAdjustment)
				}
				firstScan := time.Duration(p.LastScanAttempt.Unix()) == 0 // the database stores zero time as Unix 0, not the Go zero time
				scan.FailureRate = adjustFailureRate(p.FailureRate, firstScan, scan.Successful)

				if err := m.store.AddScan(scan); err != nil {
					log.Error("failed to record peer scan result", zap.Error(err))
				}
			})
		}

		// wait for scans to complete before fetching more peers for
		// scanning since the next scan time is not updated until after
		// the scan completes
		wg.Wait()
	}
	return nil
}

// Close shuts down the peer manager.
func (m *Manager) Close() error {
	m.tg.Stop()
	return nil
}

// Peers retrieves a list of peers from the database with the specified offset and limit.
func (m *Manager) Peers(offset, limit int) ([]Peer, error) {
	return m.store.Peers(offset, limit)
}

// BootstrapPeers returns a list of peers suitable for bootstrapping a new node.
// The final list selects reliable peers with a minimum geographic diversity.
func (m *Manager) BootstrapPeers(limit int) (peers []string, err error) {
	log := m.log.Named("bootstrap")
	for i := 0; len(peers) < limit; i += limit {
		batch, err := m.store.Peers(i*limit, limit)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve peers for bootstrap list: %w", err)
		} else if len(batch) == 0 {
			break
		}

		ss := geoip.NewSpacedSet(50) // require 50km spacing
		for _, p := range batch {
			log := log.With(zap.String("peer", p.Address))
			switch {
			case p.FailureRate > 0.75:
				log.Debug("skipping unreliable peer for bootstrap list", zap.Float64("failureRate", p.FailureRate))
				continue // skip unreliable peers
			case time.Since(p.LastSuccessfulScan) > 24*time.Hour:
				log.Debug("skipping stale peer for bootstrap list", zap.Time("lastSuccessfulScan", p.LastSuccessfulScan))
				continue // skip stale peers

			}

			locations, err := m.store.PeerLocations(p.Address)
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve peer locations for %q: %w", p.Address, err)
			} else if len(locations) == 0 {
				log.Debug("skipping peer with no location data for bootstrap list")
				continue // skip peers with no location data
			} else if !ss.Add(locations) {
				log.Debug("skipping peer too close to existing bootstrap peer", zap.Stringers("locations", locations))
				continue // skip peers that are too close to existing peers
			}
			peers = append(peers, p.Address)
		}
	}
	return
}

// NewManager creates a new peer manager, initializing it with the provided
func NewManager(explorer Explorer, locator geoip.Locator, genesisState consensus.State, genesisID types.BlockID, bootstrapPeers []string, store Store, options ...Option) (*Manager, error) {
	for _, addr := range bootstrapPeers {
		if _, err := store.AddPeer(addr); err != nil {
			return nil, fmt.Errorf("failed to add bootstrap peer %q: %w", addr, err)
		}
	}
	m := &Manager{
		tg:    threadgroup.New(),
		store: store,

		explorer:     explorer,
		locator:      locator,
		genesisState: genesisState,
		genesisID:    genesisID,
		scanInterval: 3 * time.Hour,
		scanThreads:  runtime.NumCPU(),

		log: zap.NewNop(),
	}
	for _, option := range options {
		option(m)
	}

	ctx, cancel, err := m.tg.AddContext(context.Background())
	if err != nil {
		return nil, err
	}
	go func() {
		defer cancel()

		// the timer is purposefully a short delay between checking for nodes
		// to scan, not the scan interval. The scan interval is handled
		// by the database and per-node scheduling.
		t := time.NewTimer(time.Second)
		defer t.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
			}

			m.log.Debug("starting peer scan cycle")
			if err := m.scanPeers(ctx); err != nil {
				m.log.Error("peer scanning failed", zap.Error(err))
			}
			t.Reset(time.Second)
			m.log.Debug("completed peer scan cycle")
		}
	}()

	return m, nil
}

func adjustFailureRate(current float64, first, success bool) float64 {
	const emaAlpha = 0.2
	sample := 1.0
	if success {
		sample = 0.0
	}
	if first {
		return sample
	}
	return emaAlpha*sample + (1.0-emaAlpha)*current
}
