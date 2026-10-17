package flowcontext

import (
	"testing"

	peerpkg "github.com/HoosatNetwork/HTND/v2/app/protocol/peer"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/id"
)

// TestBroadcastExceptLeavesOutExcludedPeer pins the connections a broadcast goes to: every ready peer for
// Broadcast, and every ready peer but the excluded one for BroadcastExcept.
func TestBroadcastExceptLeavesOutExcludedPeer(t *testing.T) {
	f := New(nil, nil, nil, &netadapter.NetAdapter{}, nil)
	peers := make([]*peerpkg.Peer, 3)
	for i := range peers {
		peerID, err := id.GenerateID()
		if err != nil {
			t.Fatalf("GenerateID: %+v", err)
		}
		peers[i] = peerpkg.New(&netadapter.NetConnection{})
		f.peers[*peerID] = peers[i]
	}

	check := func(excluded *peerpkg.Peer, want ...*peerpkg.Peer) {
		got := make(map[*netadapter.NetConnection]bool)
		for _, connection := range f.readyPeerConnections(excluded) {
			got[connection] = true
		}
		if len(got) != len(want) {
			t.Fatalf("broadcast goes to %d peers, want %d", len(got), len(want))
		}
		for _, peer := range want {
			if !got[peer.Connection()] {
				t.Fatalf("broadcast leaves out a peer it should reach")
			}
		}
	}
	check(nil, peers...)
	check(peers[1], peers[0], peers[2])
}
