package rpchandlers

import (
	"testing"

	"github.com/HoosatNetwork/HTND/v2/app/appmessage"
	"github.com/HoosatNetwork/HTND/v2/app/rpc/rpccontext"
	"github.com/HoosatNetwork/HTND/v2/domain"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
	"github.com/HoosatNetwork/HTND/v2/domain/miningmanager"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/config"
	"github.com/pkg/errors"
)

type emptyMempool struct{ miningmanager.MiningManager }

func (emptyMempool) GetTransactionNoClone(*externalapi.DomainTransactionID, bool, bool,
) (*externalapi.DomainTransaction, bool, bool) {
	return nil, false, false
}

// scanCountingConsensus knows no transaction and counts how often the block-store scan is asked for.
type scanCountingConsensus struct {
	externalapi.Consensus
	scans int
}

func (c *scanCountingConsensus) GetVirtualSelectedParent() (*externalapi.DomainHash, error) {
	return nil, errors.New("no chain") // the recent-acceptance walk finds nothing
}

func (c *scanCountingConsensus) GetBlockByTransactionID(*externalapi.DomainTransactionID,
) (*externalapi.DomainBlock, error) {
	c.scans++
	return nil, consensus.ErrTransactionNotInAnyBlock
}

type scanCountingDomain struct {
	domain.Domain
	consensus *scanCountingConsensus
}

func (d scanCountingDomain) MiningManager() miningmanager.MiningManager { return emptyMempool{} }
func (d scanCountingDomain) Consensus() externalapi.Consensus           { return d.consensus }

func transactionStatusForUnknownID(t *testing.T, safeRPC bool) (*appmessage.GetTransactionStatusResponseMessage, int) {
	cfg := config.DefaultConfig()
	cfg.SafeRPC = safeRPC
	fakeConsensus := &scanCountingConsensus{}
	context := &rpccontext.Context{Config: cfg, Domain: scanCountingDomain{consensus: fakeConsensus}}

	unknownID := "0000000000000000000000000000000000000000000000000000000000000001"
	response, err := HandleGetTransactionStatus(context, nil, appmessage.NewGetTransactionStatusRequestMessage(unknownID))
	if err != nil {
		t.Fatalf("HandleGetTransactionStatus: %+v", err)
	}
	return response.(*appmessage.GetTransactionStatusResponseMessage), fakeConsensus.scans
}

// TestGetTransactionStatusDoesNotScanUnderSafeRPC pins that --saferpc keeps GetTransactionStatus away
// from the block-store scan, as it keeps GetBlockByTransactionID away from it. The scan reads every
// block the node holds, so without the gate any client could start one per call with a random id.
func TestGetTransactionStatusDoesNotScanUnderSafeRPC(t *testing.T) {
	response, scans := transactionStatusForUnknownID(t, true)
	if scans != 0 {
		t.Fatalf("safe RPC mode ran the block-store scan %d time(s)", scans)
	}
	if response.Error == nil {
		t.Fatalf("expected an error saying the block search is disabled, got status %s", response.Status)
	}
	if response.Status == appmessage.TransactionStatusNotFound {
		t.Fatalf("a search that was not run must not report NOT_FOUND")
	}
}

// TestGetTransactionStatusScansWithoutSafeRPC is the control: without --saferpc the behaviour is
// unchanged, one scan and NOT_FOUND for a transaction no block holds.
func TestGetTransactionStatusScansWithoutSafeRPC(t *testing.T) {
	response, scans := transactionStatusForUnknownID(t, false)
	if scans != 1 {
		t.Fatalf("expected exactly one block-store scan, got %d", scans)
	}
	if response.Error != nil || response.Status != appmessage.TransactionStatusNotFound {
		t.Fatalf("expected NOT_FOUND without error, got status %s, error %v", response.Status, response.Error)
	}
}
