package consensus

import (
	"context"
	"sync"

	"github.com/HoosatNetwork/HTND/v2/domain/consensus/processes/pruningmanager"
	"github.com/pkg/errors"
)

// pruningPointUTXOSetScanner runs, with Config.DeferPruningPointUTXOSetUpdate, the check of the pruning point UTXO
// set against its commitment that finishes a pruning point UTXO set update (PruningManager.ScanPruningPointUTXOSet).
// It runs on its own goroutine; only the full-set hash walk runs without the consensus lock. One check runs at a
// time, and the requests made meanwhile coalesce into one more.
type pruningPointUTXOSetScanner struct {
	requests   chan struct{}
	onMismatch func(error)

	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
}

func (s *consensus) requestPruningPointUTXOSetScan() {
	select {
	case s.pruningPointUTXOSetScanner.requests <- struct{}{}:
	default:
	}
}

func (s *consensus) startPruningPointUTXOSetScanner() {
	scanner := &s.pruningPointUTXOSetScanner
	ctx, cancel := context.WithCancel(context.Background())
	scanner.cancel = cancel
	scanner.done = make(chan struct{})
	spawn("pruningPointUTXOSetScanner", func() {
		defer close(scanner.done)
		for {
			select {
			case <-ctx.Done():
				return
			case <-scanner.requests:
			}
			err := s.pruningManager.ScanPruningPointUTXOSet(ctx, s.lock)
			if errors.Is(err, pruningmanager.ErrPruningPointUTXOSetMismatch) {
				log.Criticalf("%s", err)
				if scanner.onMismatch == nil {
					// Never keep running on a set that does not match the chain.
					panic(err)
				}
				scanner.onMismatch(err)
				return
			}
			if err != nil {
				log.Errorf("The pruning point UTXO set check failed, and is retried after the next block: %+v", err)
			}
		}
	})
}

// Close stops the pruning point UTXO set check and waits for it to return, so the database can be closed after it. A
// check it interrupts leaves the update pending, and the next start checks the set again.
func (s *consensus) Close() {
	scanner := &s.pruningPointUTXOSetScanner
	scanner.closeOnce.Do(func() {
		if scanner.cancel != nil {
			scanner.cancel()
			<-scanner.done
		}
	})
}
