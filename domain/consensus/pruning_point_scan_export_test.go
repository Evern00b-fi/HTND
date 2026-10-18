package consensus

import (
	"context"

	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
)

// ScanPruningPointUTXOSetForTest runs one scan synchronously so tests can inspect
// its error. The background scanner must have been stopped with Close first.
func ScanPruningPointUTXOSetForTest(c externalapi.Consensus) error {
	s := c.(*consensus)
	return s.pruningManager.ScanPruningPointUTXOSet(context.Background(), s.lock)
}
