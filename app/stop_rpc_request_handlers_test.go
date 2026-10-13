package app

import (
	"strings"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/rpc"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/db/database/pebble"
)

// TestStopWaitsForRunningRPCRequestHandlers pins that ComponentManager.Stop waits for RPC request handlers that are
// still running, and does so before it shuts down the consensus events, which such a handler may still produce.
// htndApp.main closes the database as soon as Stop returns true.
func TestStopWaitsForRunningRPCRequestHandlers(t *testing.T) {
	componentManager, cleanup := newStoppableComponentManager(t)
	defer cleanup()

	entered := make(chan struct{})
	release := make(chan struct{})
	originalStop := stopRPCRequestHandlers
	stopRPCRequestHandlers = func(manager *rpc.Manager, timeout time.Duration) bool {
		close(entered)
		<-release // a handler that is still running
		return originalStop(manager, timeout)
	}
	defer func() { stopRPCRequestHandlers = originalStop }()

	stopped := make(chan bool, 1)
	go func() { stopped <- componentManager.Stop() }()
	select {
	case <-entered:
	case <-stopped:
		t.Fatalf("Stop returned without waiting for the RPC request handlers")
	case <-time.After(30 * time.Second):
		t.Fatalf("Stop did not wait for the RPC request handlers")
	}

	select {
	case <-stopped:
		t.Fatalf("Stop returned while an RPC request handler was still running")
	default:
	}
	requireConsensusEventsChannelOpen(t, componentManager)

	close(release)
	select {
	case ok := <-stopped:
		if !ok {
			t.Fatalf("Stop reported failure although the RPC request handlers returned")
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("Stop did not return after the RPC request handlers did")
	}
}

// TestStopReportsRPCRequestHandlersTimeout pins that Stop reports failure, so that its caller does not close the
// database, when RPC request handlers are still running after the wait for them timed out. It must do so before it
// shuts down the consensus events, which such a handler may still produce.
func TestStopReportsRPCRequestHandlersTimeout(t *testing.T) {
	componentManager, cleanup := newStoppableComponentManager(t)
	defer cleanup()

	originalStop := stopRPCRequestHandlers
	stopRPCRequestHandlers = func(*rpc.Manager, time.Duration) bool { return false } // the wait timed out
	defer func() { stopRPCRequestHandlers = originalStop }()

	if componentManager.Stop() {
		t.Fatalf("Stop reported that everything finished although an RPC request handler was still running")
	}
	requireConsensusEventsChannelOpen(t, componentManager)
	// What Stop left alone, so that cleanup can wait for the consensus events handler.
	close(componentManager.protocolManager.Context().Domain().ConsensusEventsChannel())
}

func requireConsensusEventsChannelOpen(t *testing.T, componentManager *ComponentManager) {
	select {
	case _, ok := <-componentManager.protocolManager.Context().Domain().ConsensusEventsChannel():
		if !ok {
			t.Fatalf("Stop closed the consensus events channel while an RPC request handler was still running")
		}
	default:
	}
}

// newStoppableComponentManager builds a real component manager on a temporary database, with no listeners and no
// peers, ready for Stop. cleanup waits for its consensus events handler and closes the database.
func newStoppableComponentManager(t *testing.T) (*ComponentManager, func()) {
	cfg := config.DefaultConfig()
	cfg.AppDir = t.TempDir()
	cfg.DisableDNSSeed = true
	cfg.TargetOutboundPeers = 0
	db, err := pebble.NewPebbleDB(t.TempDir(), 8)
	if err != nil {
		t.Fatalf("NewPebbleDB: %+v", err)
	}
	componentManager, err := NewComponentManager(cfg, db, make(chan struct{}))
	if err != nil {
		t.Fatalf("NewComponentManager: %+v", err)
	}
	// As in TestStopReportsConsensusEventsDrainTimeout: Stop wakes the connections loop and blocks until the loop
	// takes the wakeup, so the loop has to be waiting for it first. The net adapter is left unstarted.
	componentManager.connectionManager.Start()
	for !strings.Contains(goroutineStacks(), "(*ConnectionManager).waitTillNextIteration") {
		time.Sleep(time.Millisecond)
	}
	rpcManager := componentManager.rpcManager
	return componentManager, func() {
		if !rpcManager.WaitForConsensusEventsHandler(10 * time.Second) {
			t.Errorf("the consensus events handler did not exit")
			return
		}
		if err := db.Close(); err != nil {
			t.Errorf("Close: %+v", err)
		}
	}
}
