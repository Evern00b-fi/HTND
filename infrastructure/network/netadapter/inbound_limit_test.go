package netadapter

import (
	"net"
	"sync"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/router"
)

// startLimitTestAdapter starts a net adapter on a free loopback port with the given --maxinpeers.
func startLimitTestAdapter(t *testing.T, maxInbound int) (*NetAdapter, string) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("allocate port: %+v", err)
	}
	address := listener.Addr().String()
	listener.Close()

	cfg := config.DefaultConfig()
	cfg.Listeners = []string{address}
	cfg.MaxInboundPeers = maxInbound
	adapter, err := NewNetAdapter(cfg)
	if err != nil {
		t.Fatalf("NewNetAdapter: %+v", err)
	}
	adapter.SetP2PRouterInitializer(func(*router.Router, *NetConnection) {})
	adapter.SetRPCRouterInitializer(func(*router.Router, *NetConnection) {})
	if err := adapter.Start(); err != nil {
		t.Fatalf("Start: %+v", err)
	}
	t.Cleanup(func() { _ = adapter.Stop() })
	return adapter, address
}

// waitForCount waits until count() is want, and fails if it ever exceeds atMost meanwhile.
func waitForCount(t *testing.T, what string, count func() int, want, atMost int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		got := count()
		if got > atMost {
			t.Fatalf("%s: %d connections, more than %d", what, got, atMost)
		}
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: %d connections; want %d", what, got, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// holdCount fails if count() differs from want at any point during d.
func holdCount(t *testing.T, what string, count func() int, want int, d time.Duration) {
	t.Helper()
	for deadline := time.Now().Add(d); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if got := count(); got != want {
			t.Fatalf("%s: %d connections; want %d", what, got, want)
		}
	}
}

// TestP2PServerEnforcesMaxInboundPeersAtAdmission: --maxinpeers was only enforced by the connection
// manager's periodic pass, while the P2P gRPC server admitted any number of inbound streams, each with
// its own router and buffers. Now exactly N are admitted, the N+1st is refused, and a freed slot is
// reused.
func TestP2PServerEnforcesMaxInboundPeersAtAdmission(t *testing.T) {
	const maxInbound = 2
	server, address := startLimitTestAdapter(t, maxInbound)

	var clients []*NetAdapter
	for i := 0; i < maxInbound; i++ {
		client, _ := startLimitTestAdapter(t, maxInbound)
		if err := client.P2PConnect(address); err != nil {
			t.Fatalf("P2PConnect %d: %+v", i, err)
		}
		clients = append(clients, client)
		waitForCount(t, "server after admission", server.P2PConnectionCount, i+1, maxInbound)
	}

	refused, _ := startLimitTestAdapter(t, maxInbound)
	_ = refused.P2PConnect(address) // the refusal may surface here or only on the stream
	waitForCount(t, "refused client", refused.P2PConnectionCount, 0, 1)
	holdCount(t, "server at the limit", server.P2PConnectionCount, maxInbound, time.Second)

	for _, connection := range clients[0].P2PConnections() {
		connection.Disconnect()
	}
	waitForCount(t, "server after a peer left", server.P2PConnectionCount, maxInbound-1, maxInbound)
	reuse, _ := startLimitTestAdapter(t, maxInbound)
	if err := reuse.P2PConnect(address); err != nil {
		t.Fatalf("P2PConnect into a freed slot: %+v", err)
	}
	waitForCount(t, "server after slot reuse", server.P2PConnectionCount, maxInbound, maxInbound)
	holdCount(t, "reusing client", reuse.P2PConnectionCount, 1, 500*time.Millisecond)
}

// TestP2PServerConcurrentInboundArrivals: streams that arrive together never push the server past the
// limit, and the limit is filled.
func TestP2PServerConcurrentInboundArrivals(t *testing.T) {
	const maxInbound, arrivals = 2, 8
	server, address := startLimitTestAdapter(t, maxInbound)
	clients := make([]*NetAdapter, arrivals)
	for i := range clients {
		clients[i], _ = startLimitTestAdapter(t, maxInbound)
	}

	var wg sync.WaitGroup
	for _, client := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = client.P2PConnect(address)
		}()
	}
	wg.Wait()
	waitForCount(t, "server", server.P2PConnectionCount, maxInbound, maxInbound)
	holdCount(t, "server", server.P2PConnectionCount, maxInbound, time.Second)
}

// TestP2PServerZeroInboundStillConnectsOutbound: --maxinpeers=0 admits no inbound P2P stream, and does
// not affect this node's own outbound connections.
func TestP2PServerZeroInboundStillConnectsOutbound(t *testing.T) {
	node, address := startLimitTestAdapter(t, 0)
	client, _ := startLimitTestAdapter(t, 8)
	_ = client.P2PConnect(address)
	waitForCount(t, "refused client", client.P2PConnectionCount, 0, 1)
	holdCount(t, "node with --maxinpeers=0", node.P2PConnectionCount, 0, time.Second)

	other, otherAddress := startLimitTestAdapter(t, 8)
	if err := node.P2PConnect(otherAddress); err != nil {
		t.Fatalf("outbound P2PConnect with --maxinpeers=0: %+v", err)
	}
	waitForCount(t, "outbound side", node.P2PConnectionCount, 1, 1)
	waitForCount(t, "outbound target", other.P2PConnectionCount, 1, 1)
}
