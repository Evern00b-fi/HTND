package protowire

import (
	"bytes"
	"os"
	"os/exec"
	"runtime/debug"
	"testing"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"google.golang.org/protobuf/proto"
)

const getCurrentNetworkChildEnv = "HTND_TEST_GET_CURRENT_NETWORK_CHILD"

// TestGetCurrentNetworkResponseToAppMessage converts a GetCurrentNetworkResponse received from the
// wire. The P2P and RPC receive loops (and rpcclient) call ToAppMessage on every incoming message,
// whatever its type. This converter used to call itself until the stack overflowed, which is a fatal
// runtime error that recover cannot catch, so the conversion runs in a child process: a regression
// then fails this test instead of killing the whole test binary.
func TestGetCurrentNetworkResponseToAppMessage(t *testing.T) {
	if os.Getenv(getCurrentNetworkChildEnv) == "1" {
		debug.SetMaxStack(64 << 20) // fail in well under a second instead of at the 1 GB default
		convertGetCurrentNetworkResponse(t)
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestGetCurrentNetworkResponseToAppMessage$", "-test.count=1")
	cmd.Env = append(os.Environ(), getCurrentNetworkChildEnv+"=1")
	out, err := cmd.CombinedOutput()
	if err != nil {
		lines := bytes.SplitN(out, []byte("\n"), 4)
		t.Fatalf("converting a GetCurrentNetworkResponse killed the process (%v):\n%s", err,
			bytes.Join(lines[:min(len(lines), 3)], []byte("\n")))
	}
}

func convertGetCurrentNetworkResponse(t *testing.T) {
	wire, err := proto.Marshal(&HoosatdMessage{Payload: &HoosatdMessage_GetCurrentNetworkResponse{
		GetCurrentNetworkResponse: &GetCurrentNetworkResponseMessage{CurrentNetwork: "hoosat-mainnet"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	received := &HoosatdMessage{}
	if err := proto.Unmarshal(wire, received); err != nil {
		t.Fatal(err)
	}
	message, err := received.ToAppMessage()
	if err != nil {
		t.Fatalf("ToAppMessage: %+v", err)
	}
	response, ok := message.(*appmessage.GetCurrentNetworkResponseMessage)
	if !ok || response.CurrentNetwork != "hoosat-mainnet" {
		t.Fatalf("got %#v, want a GetCurrentNetworkResponseMessage for hoosat-mainnet", message)
	}
}
