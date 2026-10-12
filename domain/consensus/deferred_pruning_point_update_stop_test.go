package consensus_test

import (
	"math"
	"testing"
	"time"

	"github.com/HoosatNetwork/HTND/v2/domain/consensus"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/constants"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/testutils"
	"github.com/HoosatNetwork/HTND/v2/domain/prefixmanager/prefix"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/db/database"
	"github.com/HoosatNetwork/HTND/v2/infrastructure/db/database/ldb"
	"github.com/pkg/errors"
)

var errDeferredUpdateStopped = errors.New("process stopped")

// deferredStopDB simulates a node process that stops at the first transaction commit made while a pruning point UTXO
// set update is durably pending - in updatePruningPoint, the commit that deletes past blocks. With stopBefore that
// transaction is lost; otherwise it lands. Every write after the stop fails, so nothing more becomes durable.
type deferredStopDB struct {
	database.Database
	updatingKey *database.Key
	armed       bool
	stopBefore  bool
	stopped     bool
}

func (db *deferredStopDB) Put(key *database.Key, value []byte) error {
	if db.stopped {
		return errDeferredUpdateStopped
	}
	return db.Database.Put(key, value)
}

func (db *deferredStopDB) BatchPut(pairs map[*database.Key][]byte) error {
	if db.stopped {
		return errDeferredUpdateStopped
	}
	return db.Database.BatchPut(pairs)
}

func (db *deferredStopDB) Delete(key *database.Key) error {
	if db.stopped {
		return errDeferredUpdateStopped
	}
	return db.Database.Delete(key)
}

func (db *deferredStopDB) Begin() (database.Transaction, error) {
	tx, err := db.Database.Begin()
	return &deferredStopTransaction{Transaction: tx, db: db}, err
}

func (db *deferredStopDB) BeginUnindexed() (database.Transaction, error) {
	tx, err := db.Database.BeginUnindexed()
	return &deferredStopTransaction{Transaction: tx, db: db}, err
}

type deferredStopTransaction struct {
	database.Transaction
	db *deferredStopDB
}

func (tx *deferredStopTransaction) Commit() error {
	if tx.db.stopped {
		_ = tx.Transaction.Rollback()
		return errDeferredUpdateStopped
	}
	if tx.db.armed {
		updating, err := tx.db.Database.Has(tx.db.updatingKey)
		if err != nil {
			_ = tx.Transaction.Rollback()
			return err
		}
		if updating {
			tx.db.stopped = true
			if tx.db.stopBefore {
				_ = tx.Transaction.Rollback()
				return errDeferredUpdateStopped
			}
		}
	}
	return tx.Transaction.Commit()
}

// TestDeferredPruningPointUpdateSurvivesAStop stops a node with DeferPruningPointUTXOSetUpdate on, as the node's app
// sets it, at the past-block deletion commit of a deferred pruning point UTXO set update run the way the app runs it
// (UpdatePruningPointIfRequired after the insertion), then restarts it on what is durable. The restart must finish
// the update, and the set must then match the pruning point's commitment.
//
// Stopped after the deletion commit, the pending flag survives the deleted blocks the update would need to resume, on
// 2.18.2 as with this change; HoosatNetwork/HTND#38 commits the flag clear with the deletion. That case is skipped
// until this change is rebased onto #38.
func TestDeferredPruningPointUpdateSurvivesAStop(t *testing.T) {
	for _, stopBefore := range []bool{true, false} {
		name := "before-the-deletion-commit"
		if !stopBefore {
			name = "after-the-deletion-commit"
		}
		t.Run(name, func(t *testing.T) {
			if !stopBefore {
				t.Skip("needs HoosatNetwork/HTND#38: remove this skip when rebasing onto it")
			}
			testutils.ForAllNets(t, true, func(t *testing.T, consensusConfig *consensus.Config) {
				defer constants.ForceSetBlockVersion(1)
				testDeferredPruningPointUpdateSurvivesAStop(t, consensusConfig, stopBefore)
			})
		})
	}
}

func testDeferredPruningPointUpdateSurvivesAStop(t *testing.T, consensusConfig *consensus.Config, stopBefore bool) {
	consensusConfig.POWScores = []uint64{math.MaxUint64}
	consensusConfig.K = append([]externalapi.KType(nil), consensusConfig.K...)
	consensusConfig.K[0] = 0
	consensusConfig.FinalityDuration = []time.Duration{30 * consensusConfig.TargetTimePerBlock[0]}
	consensusConfig.PruningProofM = 1
	window := make([]int, len(consensusConfig.DifficultyAdjustmentWindowSize))
	for i := range window {
		window[i] = 10
	}
	consensusConfig.DifficultyAdjustmentWindowSize = window

	factory := consensus.NewFactory()
	source, teardown, err := factory.NewTestConsensus(consensusConfig, "TestDeferredPruningPointUpdateSurvivesAStop")
	if err != nil {
		t.Fatalf("NewTestConsensus: %+v", err)
	}
	defer teardown(false)

	dataDir := t.TempDir()
	db, err := ldb.NewLevelDB(dataDir, 8)
	if err != nil {
		t.Fatalf("NewLevelDB: %+v", err)
	}
	dbPrefix := &prefix.Prefix{}
	stopping := &deferredStopDB{
		Database:    db,
		updatingKey: database.MakeBucket(dbPrefix.Serialize()).Key([]byte(updatingPruningPointUTXOSetKeyName)),
		stopBefore:  stopBefore,
	}
	nodeConfig := *consensusConfig
	nodeConfig.DeferPruningPointUTXOSetUpdate = true
	node, _, err := factory.NewConsensus(&nodeConfig, stopping, dbPrefix, nil)
	if err != nil {
		t.Fatalf("NewConsensus: %+v", err)
	}

	// Arm the stop once the pruning point has moved, so that the next move's update deletes the blocks of this one.
	var firstPruningPoint *externalapi.DomainHash
	tip := consensusConfig.GenesisHash
	for i := 0; i < 300 && !stopping.stopped; i++ {
		tip, _, err = source.AddBlock([]*externalapi.DomainHash{tip}, nil, nil)
		if err != nil {
			t.Fatalf("AddBlock #%d: %+v", i, err)
		}
		block, found, err := source.GetBlock(tip)
		if err != nil || !found {
			t.Fatalf("GetBlock #%d: found %t, %+v", i, found, err)
		}
		err = node.ValidateAndInsertBlock(block, true, true)
		if err == nil {
			err = node.UpdatePruningPointIfRequired()
		}
		if err != nil && !stopping.stopped {
			t.Fatalf("block #%d: %+v", i, err)
		}
		if !stopping.armed {
			pruningPoint, err := node.PruningPoint()
			if err != nil {
				t.Fatalf("PruningPoint: %+v", err)
			}
			if !pruningPoint.Equal(consensusConfig.GenesisHash) {
				firstPruningPoint = pruningPoint
				stopping.armed = true
			}
		}
	}
	if !stopping.stopped {
		t.Fatalf("setup: no deferred pruning point UTXO set update reached its deletion commit")
	}
	updating, err := db.Has(stopping.updatingKey)
	if err != nil {
		t.Fatalf("Has: %+v", err)
	}
	if updating != stopBefore {
		t.Errorf("pruning point UTXO set update pending after the stop: %t, want %t", updating, stopBefore)
	}
	err = db.Close()
	if err != nil {
		t.Fatalf("Close: %+v", err)
	}

	// The restart: NewConsensus runs a pending update (factory.go).
	db, err = ldb.NewLevelDB(dataDir, 8)
	if err != nil {
		t.Fatalf("NewLevelDB: %+v", err)
	}
	defer db.Close()
	restarted, _, err := factory.NewConsensus(&nodeConfig, db, dbPrefix, nil)
	if err != nil {
		t.Fatalf("the node cannot restart: %+v", err)
	}
	updating, err = db.Has(stopping.updatingKey)
	if err != nil {
		t.Fatalf("Has: %+v", err)
	}
	if updating {
		t.Fatalf("the pruning point UTXO set update is still pending after the restart")
	}
	pruningPoint, err := restarted.PruningPoint()
	if err != nil {
		t.Fatalf("PruningPoint: %+v", err)
	}
	health, err := restarted.CheckUTXOHealth(pruningPoint)
	if err != nil {
		t.Fatalf("CheckUTXOHealth: %+v", err)
	}
	if !health.Verified {
		t.Errorf("the pruning point UTXO set does not match the pruning point's commitment after the restart: %+v",
			health)
	}
	info, err := restarted.GetBlockInfo(firstPruningPoint)
	if err != nil {
		t.Fatalf("GetBlockInfo: %+v", err)
	}
	if info.BlockStatus != externalapi.StatusHeaderOnly {
		t.Errorf("setup: the update did not delete the blocks of the first pruning point %s (status %s)",
			firstPruningPoint, info.BlockStatus)
	}
}
