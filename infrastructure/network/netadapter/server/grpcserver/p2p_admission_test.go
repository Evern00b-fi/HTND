package grpcserver

import (
	"net"
	"sync"
	"testing"
)

func newTestP2PServer(t *testing.T, maxInbound int, requestedPeers []string) *p2pServer {
	t.Helper()
	server, err := NewP2PServer(nil, maxInbound, requestedPeers)
	if err != nil {
		t.Fatalf("NewP2PServer: %+v", err)
	}
	return server.(*p2pServer)
}

func tcpAddress(t *testing.T, address string) *net.TCPAddr {
	t.Helper()
	tcpAddress, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		t.Fatalf("ResolveTCPAddr: %+v", err)
	}
	return tcpAddress
}

// TestP2PAdmissionConcurrentArrivals: of 20 streams arriving at once with a limit of 2, exactly 2 are
// admitted, and a freed slot is reused while the limit still holds.
func TestP2PAdmissionConcurrentArrivals(t *testing.T) {
	const maxInbound, arrivals = 2, 20
	server := newTestP2PServer(t, maxInbound, nil)

	var wg sync.WaitGroup
	var lock sync.Mutex
	var releases []func()
	for i := 0; i < arrivals; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			release, err := server.admitInbound(&net.TCPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 10000 + i})
			if err == nil {
				lock.Lock()
				releases = append(releases, release)
				lock.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(releases) != maxInbound {
		t.Fatalf("admitted %d of %d concurrent streams; want exactly %d", len(releases), arrivals, maxInbound)
	}

	releases[0]()
	if _, err := server.admitInbound(tcpAddress(t, "198.51.100.2:20000")); err != nil {
		t.Fatalf("a freed slot was not reused: %v", err)
	}
	if _, err := server.admitInbound(tcpAddress(t, "198.51.100.2:20001")); err == nil {
		t.Fatal("a stream over the limit was admitted after the freed slot was reused")
	}
}

// TestP2PAdmissionZeroAdmitsNone: --maxinpeers=0 admits no inbound P2P stream, including one from a
// configured requested peer, while RPC keeps reading a zero limit as unlimited.
func TestP2PAdmissionZeroAdmitsNone(t *testing.T) {
	server := newTestP2PServer(t, 0, []string{"203.0.113.7:42421"})
	if _, err := server.admitInbound(tcpAddress(t, "198.51.100.1:10000")); err == nil {
		t.Fatal("--maxinpeers=0 admitted an inbound P2P stream")
	}
	if _, err := server.admitInbound(tcpAddress(t, "203.0.113.7:42421")); err == nil {
		t.Fatal("--maxinpeers=0 admitted an inbound P2P stream from a requested peer")
	}

	rpc := newGRPCServer(nil, 1024, 0, "RPC")
	for i := 0; i < 5; i++ {
		if _, err := rpc.admitInbound(&net.TCPAddr{IP: net.IPv4(198, 51, 100, 1), Port: 10000 + i}); err != nil {
			t.Fatalf("an RPC server with limit 0 refused a client: %v", err)
		}
	}
}

// TestP2PAdmissionRequestedPeer: an inbound stream whose IP and port match a configured --addpeer or
// --connect address - the connections the ConnectionManager does not count as inbound peers - takes
// one slot of its own outside the limit. Only that exact address does, and only one stream at a time.
func TestP2PAdmissionRequestedPeer(t *testing.T) {
	server := newTestP2PServer(t, 1, []string{"203.0.113.7:42421", "requested.example:42421"})
	if _, err := server.admitInbound(tcpAddress(t, "198.51.100.1:10000")); err != nil {
		t.Fatalf("the first ordinary stream was refused: %v", err)
	}
	if _, err := server.admitInbound(tcpAddress(t, "198.51.100.1:10001")); err == nil {
		t.Fatal("an ordinary stream over the limit was admitted")
	}
	if _, err := server.admitInbound(tcpAddress(t, "203.0.113.7:50000")); err == nil {
		t.Fatal("a stream from the requested peer's IP but another port was admitted outside the limit")
	}

	release, err := server.admitInbound(tcpAddress(t, "203.0.113.7:42421"))
	if err != nil {
		t.Fatalf("the requested peer was refused at a full limit: %v", err)
	}
	if _, err := server.admitInbound(tcpAddress(t, "203.0.113.7:42421")); err == nil {
		t.Fatal("a second stream from the requested peer took another slot outside the limit")
	}
	release()
	if _, err := server.admitInbound(tcpAddress(t, "203.0.113.7:42421")); err != nil {
		t.Fatalf("the requested peer's freed slot was not reused: %v", err)
	}
}
