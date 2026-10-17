package protocol

import (
	"testing"

	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/router"
)

// TestRouterInitializedAfterCloseDoesNotPanic pins that a router whose flows goroutine starts after the
// protocol manager closed returns quietly. The net adapter rejects connections once it is stopped, but
// the goroutine spawned by a router initializer that ran just before can still start after Close; it
// used to panic, and the panic handler exits the process with status 1 in the middle of shutdown.
func TestRouterInitializedAfterCloseDoesNotPanic(t *testing.T) {
	// Run the flows goroutine inline, so it has finished when routerInitializer returns and a panic reaches
	// the recover below instead of the panic handler.
	originalSpawn := spawn
	spawn = func(_ string, f func()) { f() }
	defer func() { spawn = originalSpawn }()
	defer func() {
		if r := recover(); r != nil {
			t.Errorf("the flows goroutine of a router initialized after Close panicked: %v", r)
		}
	}()

	m := &Manager{}
	m.isClosed.Store(1)
	m.routerInitializer(router.NewRouter("late"), &netadapter.NetConnection{})
}
