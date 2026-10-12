package app

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/app/rpc"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/db/database"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/db/database/pebble"
	"github.com/pkg/errors"
)

const shutdownTestChildEnv = "HTND_TEST_SHUTDOWN_CHILD"

// closeRecordingDatabase stands in for the node's database; only Close is used.
type closeRecordingDatabase struct{ database.Database }

func (closeRecordingDatabase) Close() error {
	fmt.Println("database closed")
	return nil
}

// TestShutdownTimeoutDoesNotCloseDatabase pins that the database is not closed under goroutines that
// may still use it. Both shutdown timeouts used to fall through to the database Close deferred in
// htndApp.main: app.go's outer timeout logged and returned while ComponentManager.Stop kept running, and
// Stop itself returned normally when the consensus events handler outlived its drain timeout. A flow
// or the handler then hit "pebble: closed", panicked and exited with status 1 during the close.
// Each case runs in a subprocess because a timeout now exits the process.
func TestShutdownTimeoutDoesNotCloseDatabase(t *testing.T) {
	if mode := os.Getenv(shutdownTestChildEnv); mode != "" {
		runShutdownTestChild(mode)
		return
	}

	tests := []struct {
		mode       string
		wantClosed bool
	}{
		{mode: "stopped", wantClosed: true},
		{mode: "stop-timeout", wantClosed: false},
		{mode: "drain-timeout", wantClosed: false},
	}
	for _, test := range tests {
		cmd := exec.Command(os.Args[0], "-test.run=^TestShutdownTimeoutDoesNotCloseDatabase$", "-test.timeout=60s")
		cmd.Env = append(os.Environ(), shutdownTestChildEnv+"="+test.mode)
		output, err := cmd.CombinedOutput()

		if closed := strings.Contains(string(output), "database closed"); closed != test.wantClosed {
			t.Errorf("%s: database closed = %t, want %t; output:\n%s", test.mode, closed, test.wantClosed, output)
		}
		var exitErr *exec.ExitError
		if test.wantClosed && err != nil {
			t.Errorf("%s: clean shutdown exited with %v; output:\n%s", test.mode, err, output)
		}
		if !test.wantClosed && !(errors.As(err, &exitErr) && exitErr.ExitCode() != 0) {
			t.Errorf("%s: shutdown timeout did not exit non-zero (%v); output:\n%s", test.mode, err, output)
		}
	}
}

func runShutdownTestChild(mode string) {
	var stop func() bool
	switch mode {
	case "stopped":
		stop = func() bool { return true }
	case "stop-timeout":
		stop = func() bool { select {} } // a component that never stops
	case "drain-timeout":
		// A consensus events handler that never finishes, as ComponentManager.Stop waits for it.
		stop = func() bool { return (&rpc.Manager{}).WaitForConsensusEventsHandler(100 * time.Millisecond) }
	default:
		panic("unknown mode " + mode)
	}

	// The order of htndApp.main: the database Close is deferred before the components are stopped.
	defer closeRecordingDatabase{}.Close()
	stopComponents(stop, time.Second)
}

// TestStopReportsConsensusEventsDrainTimeout pins that ComponentManager.Stop reports failure when the consensus events
// handler outlives its drain timeout, so that the caller does not close the database under the handler.
func TestStopReportsConsensusEventsDrainTimeout(t *testing.T) {
	originalTimeout := consensusEventsDrainTimeout
	consensusEventsDrainTimeout = 100 * time.Millisecond
	defer func() { consensusEventsDrainTimeout = originalTimeout }()

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
	// Stop wakes the connections loop and blocks until the loop takes the wakeup, so the loop has to be waiting for
	// it first. The net adapter is left unstarted: no listeners are needed.
	componentManager.connectionManager.Start()
	for !strings.Contains(goroutineStacks(), "(*ConnectionManager).waitTillNextIteration") {
		time.Sleep(time.Millisecond)
	}

	// A consensus events handler that never finishes. The real one exits once Stop closes its channel; the database
	// is closed after that.
	rpcManager := componentManager.rpcManager
	componentManager.rpcManager = &rpc.Manager{}
	defer func() {
		if !rpcManager.WaitForConsensusEventsHandler(10 * time.Second) {
			t.Errorf("the real consensus events handler did not exit")
			return
		}
		if err := db.Close(); err != nil {
			t.Errorf("Close: %+v", err)
		}
	}()

	if componentManager.Stop() {
		t.Fatalf("Stop reported that everything finished although the consensus events handler did not")
	}
}

func goroutineStacks() string {
	buf := make([]byte, 1<<20)
	return string(buf[:runtime.Stack(buf, true)])
}
