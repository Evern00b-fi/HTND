package connmanager

import (
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter"
)

// TestStopReturnsWhenTheConnectionLoopIsNotWaiting pins that Stop never waits for the connection loop. Stop wakes the
// loop with a send on resetLoopChan, but the loop is not always there to receive it. If its 30 s ticker fires after
// Stop sets the stop flag, the loop sees the flag and returns. If it is in the middle of an iteration, it only reads
// the channel later. Stop must return in both cases, and a loop that was busy must still be woken once afterwards,
// because Stop has also stopped the ticker that would otherwise wake it.
func TestStopReturnsWhenTheConnectionLoopIsNotWaiting(t *testing.T) {
	cfg := config.DefaultConfig()
	adapter, err := netadapter.NewNetAdapter(cfg)
	if err != nil {
		t.Fatalf("NewNetAdapter: %+v", err)
	}
	connectionManager, err := New(cfg, adapter, nil)
	if err != nil {
		t.Fatalf("New: %+v", err)
	}

	// The loop is not running, so nothing receives the wake-up - the same as a loop that has returned or is busy.
	returned := make(chan struct{})
	go func() {
		connectionManager.Stop()
		connectionManager.run() // a later wake-up, as AddConnectionRequest sends
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(5 * time.Second):
		t.Fatalf("Stop blocked on a connection loop that was not waiting for its wake-up")
	}

	// A loop that was busy during Stop reaches waitTillNextIteration afterwards: it must wake up and see the stop flag.
	woke := make(chan struct{})
	go func() {
		connectionManager.waitTillNextIteration()
		close(woke)
	}()
	select {
	case <-woke:
	case <-time.After(5 * time.Second):
		t.Fatalf("a connection loop that was busy during Stop is never woken: the ticker is stopped and the wake-up was lost")
	}
	if connectionManager.stop.Load() == 0 {
		t.Fatalf("Stop did not set the stop flag")
	}
}
