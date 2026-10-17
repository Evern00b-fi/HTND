package netadapter

import (
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/router"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/server"
	"github.com/pkg/errors"
)

// stoppableServer stands in for the gRPC servers: Stop does nothing and Connect records the dial.
type stoppableServer struct {
	server.P2PServer
	dialed bool
}

func (s *stoppableServer) Stop() error { return nil }

func (s *stoppableServer) Connect(string) (server.Connection, error) {
	s.dialed = true
	return nil, errors.New("not dialing in a test")
}

// TestStopDisconnectsOutboundConnections pins that NetAdapter.Stop disconnects the P2P connections it
// has and that P2PConnect refuses to dial afterwards. Stopping the gRPC server only ends inbound
// streams, so an outbound connection used to survive shutdown with its router - and the protocol flows
// reading its routes - still running.
func TestStopDisconnectsOutboundConnections(t *testing.T) {
	stubServer := &stoppableServer{}
	na := &NetAdapter{
		p2pServer:          stubServer,
		rpcServer:          stubServer,
		p2pConnections:     make(map[*NetConnection]struct{}),
		outboundP2PRouters: make(map[string]*router.Router),
	}
	var route *router.Route
	na.SetP2PRouterInitializer(func(r *router.Router, _ *NetConnection) {
		var err error
		route, err = r.AddIncomingRoute("flow", []appmessage.MessageCommand{appmessage.CmdPing})
		if err != nil {
			t.Fatalf("AddIncomingRoute: %+v", err)
		}
	})
	connection := &disconnectableConnection{outbound: true}
	connection.connected.Store(true)
	if err := na.onP2PConnectedHandler(connection); err != nil {
		t.Fatalf("onP2PConnectedHandler: %+v", err)
	}

	if err := na.Stop(); err != nil {
		t.Fatalf("Stop: %+v", err)
	}
	if connection.IsConnected() || na.P2PConnectionCount() != 0 {
		t.Errorf("an outbound connection survived Stop")
	}
	if _, err := route.DequeueWithTimeout(time.Millisecond); !errors.Is(err, router.ErrRouteClosed) {
		t.Errorf("the connection's route is still open after Stop: %v", err)
	}
	if err := na.P2PConnect("127.0.0.1:16111"); err == nil || stubServer.dialed {
		t.Errorf("P2PConnect dialed after Stop")
	}
}

// TestP2PConnectionIsRejectedOnceStopping pins that a P2P connection reaching the connected handler
// while or after the adapter stops is disconnected and not registered. Stop only disconnects the
// connections registered when it runs, so one registered later would keep its router - and the
// protocol flows on it - running after shutdown.
func TestP2PConnectionIsRejectedOnceStopping(t *testing.T) {
	newAdapter := func() *NetAdapter {
		return &NetAdapter{
			p2pConnections:     make(map[*NetConnection]struct{}),
			outboundP2PRouters: make(map[string]*router.Router),
		}
	}
	for _, outbound := range []bool{false, true} {
		// Late router initialization: Stop starts while the router initializer is running.
		na := newAdapter()
		na.SetP2PRouterInitializer(func(*router.Router, *NetConnection) {
			na.stop.Add(1) // what Stop does first
		})
		connection := &disconnectableConnection{outbound: outbound}
		connection.connected.Store(true)
		if err := na.onP2PConnectedHandler(connection); err == nil {
			t.Fatalf("outbound=%t: a connection whose router was initialized during Stop was accepted", outbound)
		}
		if connection.IsConnected() {
			t.Fatalf("outbound=%t: a connection rejected during Stop was left connected", outbound)
		}
		if n := na.P2PConnectionCount(); n != 0 {
			t.Fatalf("outbound=%t: a connection rejected during Stop is registered (%d connections)", outbound, n)
		}
		if len(na.outboundP2PRouters) != 0 {
			t.Fatalf("outbound=%t: the router of a connection rejected during Stop is still cached", outbound)
		}

		// After Stop: the router initializer - which starts protocol flows - must not run at all.
		na = newAdapter()
		na.stop.Add(1)
		na.SetP2PRouterInitializer(func(*router.Router, *NetConnection) {
			t.Errorf("outbound=%t: router initialized for a connection made after Stop", outbound)
		})
		connection = &disconnectableConnection{outbound: outbound}
		connection.connected.Store(true)
		if err := na.onP2PConnectedHandler(connection); err == nil {
			t.Fatalf("outbound=%t: a connection made after Stop was accepted", outbound)
		}
		if connection.IsConnected() || na.P2PConnectionCount() != 0 || len(na.outboundP2PRouters) != 0 {
			t.Fatalf("outbound=%t: a connection made after Stop was kept", outbound)
		}
	}
}
