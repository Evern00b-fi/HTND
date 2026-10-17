package rpc

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/HTND/v2/app/rpc/rpccontext"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/network/netadapter/router"
)

// blockingRequestHandler replaces the GetBlockCount handler with one that runs until release is closed, and the
// GetInfo handler with one that only counts its calls.
func blockingRequestHandler(t *testing.T) (started, release chan struct{}, infoCalls *atomic.Int32) {
	started = make(chan struct{})
	release = make(chan struct{})
	infoCalls = &atomic.Int32{}
	originalBlockCountHandler := handlers[appmessage.CmdGetBlockCountRequestMessage]
	originalInfoHandler := handlers[appmessage.CmdGetInfoRequestMessage]
	handlers[appmessage.CmdGetBlockCountRequestMessage] = func(*rpccontext.Context, *router.Router,
		appmessage.Message,
	) (appmessage.Message, error) {
		close(started)
		<-release
		return &appmessage.GetBlockCountResponseMessage{}, nil
	}
	handlers[appmessage.CmdGetInfoRequestMessage] = func(*rpccontext.Context, *router.Router,
		appmessage.Message,
	) (appmessage.Message, error) {
		infoCalls.Add(1)
		return &appmessage.GetInfoResponseMessage{}, nil
	}
	t.Cleanup(func() {
		handlers[appmessage.CmdGetBlockCountRequestMessage] = originalBlockCountHandler
		handlers[appmessage.CmdGetInfoRequestMessage] = originalInfoHandler
	})
	return started, release, infoCalls
}

// TestShutdownWaitsForRunningRequestHandlers pins that shutdown does not close the database under an RPC request
// handler that is still running, such as one iterating the block store. Stopping the net adapter closes the RPC
// connections without waiting for their handlers, and the database was closed right after: the race detector caught
// the database close closing a cursor such a handler was still using.
func TestShutdownWaitsForRunningRequestHandlers(t *testing.T) {
	started, release, infoCalls := blockingRequestHandler(t)
	manager := &Manager{context: &rpccontext.Context{}}
	rtr := router.NewRouter("TestShutdownWaitsForRunningRequestHandlers")
	defer rtr.Close()

	handlerReturned := make(chan struct{})
	go func() {
		defer close(handlerReturned)
		_ = manager.handleRequest(rtr, appmessage.NewGetBlockCountRequestMessage())
	}()
	<-started

	// What htndApp.main does on shutdown: ComponentManager.Stop, which stops the request handlers, then closes the
	// database.
	stopped := make(chan bool, 1)
	databaseClosed := make(chan struct{})
	go func() {
		stopped <- manager.StopRequestHandlers(10 * time.Second)
		close(databaseClosed)
	}()

	// Release the handler once shutdown is waiting for it, which shows in requests no longer being handled, or once
	// the database is closed.
	deadline := time.After(10 * time.Second)
waitForShutdown:
	for {
		select {
		case <-databaseClosed:
			break waitForShutdown
		case <-deadline:
			t.Fatalf("shutdown neither stopped handling requests nor closed the database")
		default:
		}
		before := infoCalls.Load()
		_ = manager.handleRequest(rtr, appmessage.NewGetInfoRequestMessage())
		if infoCalls.Load() == before {
			break
		}
		time.Sleep(time.Millisecond)
	}

	select {
	case <-databaseClosed:
		t.Fatalf("the database was closed while an RPC request handler was still running")
	default:
	}
	close(release)
	<-handlerReturned
	select {
	case ok := <-stopped:
		if !ok {
			t.Fatalf("StopRequestHandlers reported a handler still running after it returned")
		}
	case <-time.After(10 * time.Second):
		t.Fatalf("StopRequestHandlers did not return after the last handler did")
	}
	<-databaseClosed
}

// TestStopRequestHandlersIsBounded pins that the wait gives up after its timeout and reports that a handler is still
// running.
func TestStopRequestHandlersIsBounded(t *testing.T) {
	started, release, _ := blockingRequestHandler(t)
	manager := &Manager{context: &rpccontext.Context{}}
	rtr := router.NewRouter("TestStopRequestHandlersIsBounded")
	defer rtr.Close()

	handlerReturned := make(chan struct{})
	go func() {
		defer close(handlerReturned)
		_ = manager.handleRequest(rtr, appmessage.NewGetBlockCountRequestMessage())
	}()
	<-started
	defer func() {
		close(release)
		<-handlerReturned
	}()

	if manager.StopRequestHandlers(50 * time.Millisecond) {
		t.Fatalf("StopRequestHandlers reported every handler returned while one was still running")
	}
}
