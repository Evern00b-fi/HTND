package connmanager

import (
	"sync"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/router"
	"github.com/pkg/errors"
)

// TestOutboundConnectionDialedDuringStopDoesNotSurviveShutdown pins that an outbound connection the
// connections loop dials after ConnectionManager.Stop's disconnect sweep is closed by NetAdapter.Stop.
// Stopping the gRPC server only ends inbound streams, so this connection used to survive shutdown with
// its router open: protocol.Manager.Close waited for it until app.go's shutdown timeout fired.
func TestOutboundConnectionDialedDuringStopDoesNotSurviveShutdown(t *testing.T) {
	addrA := freeLocalAddress(t)
	addrB := freeLocalAddress(t)

	cfgA := config.DefaultConfig()
	cfgA.Listeners = []string{addrA}
	adapterA, err := netadapter.NewNetAdapter(cfgA)
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var routeA *router.Route
	adapterA.SetP2PRouterInitializer(func(r *router.Router, _ *netadapter.NetConnection) {
		route, err := r.AddIncomingRoute("flow", []appmessage.MessageCommand{appmessage.CmdPing})
		if err != nil {
			t.Errorf("AddIncomingRoute: %v", err)
			return
		}
		mu.Lock()
		routeA = route
		mu.Unlock()
	})
	adapterA.SetRPCRouterInitializer(func(*router.Router, *netadapter.NetConnection) {})
	if err := adapterA.Start(); err != nil {
		t.Fatal(err)
	}
	startNetAdapterForTest(t, addrB)

	cm := &ConnectionManager{
		cfg:           cfgA,
		netAdapter:    adapterA,
		resetLoopChan: make(chan struct{}, 1), // as New makes it
		loopTicker:    time.NewTicker(time.Hour),
	}

	// Stand-in for connectionsLoop with a dial in flight when Stop is called: the dial completes after
	// Stop's disconnect sweep. Stop wakes the loop only after the sweep, so the dial is made after taking
	// that wakeup.
	loopDone := make(chan struct{})
	go func() {
		defer close(loopDone)
		cm.waitTillNextIteration()
		if err := cm.initiateConnection(addrB); err != nil {
			t.Errorf("initiateConnection after Stop's sweep: %v", err)
		}
	}()

	cm.Stop() // ComponentManager.Stop order: connectionManager.Stop ...
	<-loopDone
	if n := adapterA.P2PConnectionCount(); n != 1 {
		t.Fatalf("expected the late outbound connection to be up before NetAdapter.Stop, got %d connections", n)
	}
	if err := adapterA.Stop(); err != nil { // ... then netAdapter.Stop
		t.Fatal(err)
	}

	if n := adapterA.P2PConnectionCount(); n != 0 {
		t.Fatalf("%d P2P connection(s) survived NetAdapter.Stop", n)
	}
	mu.Lock()
	route := routeA
	mu.Unlock()
	if _, err := route.DequeueWithTimeout(time.Second); !errors.Is(err, router.ErrRouteClosed) {
		t.Fatalf("the connection's route is still open after NetAdapter.Stop: %v", err)
	}
}
