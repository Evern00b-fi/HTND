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

var errStopped = errors.New("process stopped")

// stoppingDatabase simulates a node process that stops at the first transaction commit made while the pruning point
// UTXO set update flag is durably set - in updatePruningPoint, the commit that deletes past blocks. With stopBefore
// that transaction is lost; otherwise it lands. Every write after the stop fails, so nothing more becomes durable.
type stoppingDatabase struct {
	database.Database
	flagKey    *database.Key
	armed      bool
	stopBefore bool
	stopped    bool
}

func (db *stoppingDatabase) Put(key *database.Key, value []byte) error {
	if db.stopped {
		return errStopped
	}
	return db.Database.Put(key, value)
}

func (db *stoppingDatabase) BatchPut(pairs map[*database.Key][]byte) error {
	if db.stopped {
		return errStopped
	}
	return db.Database.BatchPut(pairs)
}

func (db *stoppingDatabase) Delete(key *database.Key) error {
	if db.stopped {
		return errStopped
	}
	return db.Database.Delete(key)
}

func (db *stoppingDatabase) Begin() (database.Transaction, error) {
	tx, err := db.Database.Begin()
	return &stoppingTransaction{Transaction: tx, db: db}, err
}

func (db *stoppingDatabase) BeginUnindexed() (database.Transaction, error) {
	tx, err := db.Database.BeginUnindexed()
	return &stoppingTransaction{Transaction: tx, db: db}, err
}

type stoppingTransaction struct {
	database.Transaction
	db *stoppingDatabase
}

func (tx *stoppingTransaction) Commit() error {
	if tx.db.stopped {
		_ = tx.Transaction.Rollback()
		return errStopped
	}
	if tx.db.armed {
		updating, err := tx.db.Database.Has(tx.db.flagKey)
		if err != nil {
			_ = tx.Transaction.Rollback()
			return err
		}
		if updating {
			tx.db.stopped = true
			if tx.db.stopBefore {
				_ = tx.Transaction.Rollback()
				return errStopped
			}
		}
	}
	return tx.Transaction.Commit()
}

// TestStopDuringPruningPointUpdateRestarts stops a node at the commit of a pruning point update that deletes past
// blocks, then restarts it on what is durable. Stopped just after that commit, the update must already be complete,
// since the blocks its resumption would need are gone; stopped just before it, the update must still be pending and
// resume on restart. The network parameters are shrunk so the deletion really reaches the previous pruning point.
func TestStopDuringPruningPointUpdateRestarts(t *testing.T) {
	for _, stopBefore := range []bool{false, true} {
		name := "after-the-deletion-commit"
		if stopBefore {
			name = "before-the-deletion-commit"
		}
		t.Run(name, func(t *testing.T) {
			testStopDuringPruningPointUpdate(t, stopBefore)
		})
	}
}

func testStopDuringPruningPointUpdate(t *testing.T, stopBefore bool) {
	testutils.ForAllNets(t, true, func(t *testing.T, consensusConfig *consensus.Config) {
		defer constants.ForceSetBlockVersion(1)

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
		blockSource, teardown, err := factory.NewTestConsensus(consensusConfig, "TestStopDuringPruningPointUpdate")
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
		stopping := &stoppingDatabase{
			Database:   db,
			flagKey:    database.MakeBucket(dbPrefix.Serialize()).Key([]byte("updating-pruning-point-utxo-set")),
			stopBefore: stopBefore,
		}
		node, _, err := factory.NewConsensus(consensusConfig, stopping, dbPrefix, nil)
		if err != nil {
			t.Fatalf("NewConsensus: %+v", err)
		}

		// Arm the stop once the pruning point has moved, so the next move deletes the blocks of the previous one.
		var previousPruningPoint *externalapi.DomainHash
		tip := consensusConfig.GenesisHash
		for i := 0; i < 300 && !stopping.stopped; i++ {
			tip, _, err = blockSource.AddBlock([]*externalapi.DomainHash{tip}, nil, nil)
			if err != nil {
				t.Fatalf("AddBlock #%d: %+v", i, err)
			}
			block, found, err := blockSource.GetBlock(tip)
			if err != nil || !found {
				t.Fatalf("GetBlock #%d: found %t, %+v", i, found, err)
			}
			err = node.ValidateAndInsertBlock(block, true, true)
			if err != nil && !stopping.stopped {
				t.Fatalf("ValidateAndInsertBlock #%d: %+v", i, err)
			}
			if !stopping.armed {
				pruningPoint, err := node.PruningPoint()
				if err != nil {
					t.Fatalf("PruningPoint: %+v", err)
				}
				if !pruningPoint.Equal(consensusConfig.GenesisHash) {
					previousPruningPoint = pruningPoint
					stopping.armed = true
				}
			}
		}
		if !stopping.stopped {
			t.Fatalf("setup: no pruning point update reached its deletion commit")
		}

		updating, err := db.Has(stopping.flagKey)
		if err != nil {
			t.Fatalf("Has: %+v", err)
		}
		if updating != stopBefore {
			t.Errorf("pruning point update pending after the stop: %t, want %t", updating, stopBefore)
		}
		if err := db.Close(); err != nil {
			t.Fatalf("Close: %+v", err)
		}

		// The restart: NewConsensus resumes a pending update (factory.go, UpdatePruningPointIfRequired).
		db, err = ldb.NewLevelDB(dataDir, 8)
		if err != nil {
			t.Fatalf("NewLevelDB: %+v", err)
		}
		defer db.Close()
		restarted, _, err := factory.NewConsensus(consensusConfig, db, dbPrefix, nil)
		if err != nil {
			t.Fatalf("the node cannot restart: %+v", err)
		}
		updating, err = db.Has(stopping.flagKey)
		if err != nil {
			t.Fatalf("Has: %+v", err)
		}
		if updating {
			t.Errorf("the pruning point update is still pending after the restart")
		}
		info, err := restarted.GetBlockInfo(previousPruningPoint)
		if err != nil {
			t.Fatalf("GetBlockInfo: %+v", err)
		}
		if info.BlockStatus != externalapi.StatusHeaderOnly {
			t.Errorf("setup: the update did not delete the previous pruning point %s (status %s)",
				previousPruningPoint, info.BlockStatus)
		}
	})
}
