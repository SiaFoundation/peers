package peers

import (
	"context"
	"fmt"
	"net"
	"time"

	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
)

// withGatewayTransport dials the given address and performs a gateway handshake,
// then calls the provided function with the established transport.
func withGatewayTransport(ctx context.Context, addr string, genesisID types.BlockID, fn func(*gateway.Transport) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := (&net.Dialer{}).DialContext(ctx, "tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to connect to %s: %w", addr, err)
	}
	defer conn.Close()

	go func() {
		// force close the connection when the context is done
		<-ctx.Done()
		conn.Close()
	}()

	transport, err := gateway.Dial(conn, gateway.Header{
		GenesisID:  genesisID,
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: "ephemeral:0",
	})
	if err != nil {
		return fmt.Errorf("failed to perform gateway handshake with %s: %w", addr, err)
	}
	defer transport.Close()

	return fn(transport)
}

func callRPC(transport *gateway.Transport, r gateway.Object, timeout time.Duration) error {
	s, err := transport.DialStream()
	if err != nil {
		return fmt.Errorf("couldn't open stream: %w", err)
	}
	defer s.Close()
	s.SetDeadline(time.Now().Add(timeout))
	if err := s.WriteID(r); err != nil {
		return fmt.Errorf("couldn't write RPC ID: %w", err)
	} else if err := s.WriteRequest(r); err != nil {
		return fmt.Errorf("couldn't write request: %w", err)
	} else if err := s.ReadResponse(r); err != nil {
		return fmt.Errorf("couldn't read response: %w", err)
	}
	return nil
}

// sharePeers requests a list of peers from the given transport.
func sharePeers(transport *gateway.Transport, timeout time.Duration) ([]string, error) {
	r := &gateway.RPCShareNodes{}
	err := callRPC(transport, r, timeout)
	return r.Peers, err
}

// getHeaders requests up to n headers from p, starting from the supplied
// index, which must be on the peer's best chain. The peer also returns the
// number of remaining headers left to sync.
func getHeaders(transport *gateway.Transport, cs consensus.State, maxHeaders uint64, timeout time.Duration) ([]types.BlockHeader, uint64, error) {
	r := &gateway.RPCSendHeaders{Index: cs.Index, Max: maxHeaders}
	err := callRPC(transport, r, timeout)
	if err != nil {
		return nil, 0, err
	}
	for _, bh := range r.Headers {
		if err := consensus.ValidateHeader(cs, bh); err != nil {
			return nil, 0, fmt.Errorf("peer sent invalid header %v: %w", bh.ID(), err)
		}
		cs = consensus.ApplyHeader(cs, bh, time.Time{})
	}
	return r.Headers, r.Remaining, nil
}
