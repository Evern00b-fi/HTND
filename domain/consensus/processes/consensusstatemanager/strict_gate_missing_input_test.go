package consensusstatemanager_test

import (
	"testing"

	"github.com/HoosatNetwork/HTND/v2/domain/consensus"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/consensushashing"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/merkle"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/testutils"
	"github.com/HoosatNetwork/HTND/v2/util/staging"
)

// TestStrictGateEndsMissingInputTolerance: with the strict UTXO gates active, a block whose body spends
// an input that does not exist is disqualified - also when its selected parent is this node\x27s pruning
// point and the pruning-point UTXO baseline is verified. The missing-input tolerance in
// validateBlockTransactionsAgainstPastUTXO did not consult the strict gate, and the offset predicate is
// true for a child of the pruning point, so on that boundary such a block resolved as valid. No UTXO
// or multiset corruption is injected; only the local pruning point differs between the two cases.
// The gates are forced active and proof of work is skipped: this pins a validation rule, it does not
// show how a remote party would get such a block to this point.
func TestStrictGateEndsMissingInputTolerance(t *testing.T) {
	testutils.ForAllNets(t, true, func(t *testing.T, cfg *consensus.Config) {
		for _, boundary := range []bool{false, true} {
			name := "ordinary-parent"
			if boundary {
				name = "pruning-boundary"
			}
			t.Run(name, func(t *testing.T) {
				tc, teardown, err := consensus.NewFactory().NewTestConsensus(cfg, "strict-gate-missing-input")
				if err != nil {
					t.Fatal(err)
				}
				defer teardown(false)

				parent, _, err := tc.AddBlock([]*externalapi.DomainHash{cfg.GenesisHash}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				block, _, err := tc.BuildBlockWithParents([]*externalapi.DomainHash{parent}, nil, nil)
				if err != nil {
					t.Fatal(err)
				}
				block.Transactions = append(block.Transactions, &externalapi.DomainTransaction{
					Inputs: []*externalapi.DomainTransactionInput{{
						PreviousOutpoint: externalapi.DomainOutpoint{Index: 777},
						Sequence:         ^uint64(0),
					}},
					Outputs: []*externalapi.DomainTransactionOutput{{
						Value:           1,
						ScriptPublicKey: &externalapi.ScriptPublicKey{Script: []byte{0x51}},
					}},
				})
				header := block.Header.ToMutable()
				header.SetHashMerkleRoot(merkle.CalculateHashMerkleRoot(block.Transactions))
				block.Header = header.ToImmutable()

				tc.HardForkGates().StrictUTXOCommitmentVersion = 1
				tc.HardForkGates().StrictMinersViewFieldsVersion = 1
				tc.HardForkGates().StrictCoinbaseVersion = 1

				// Insert the block without resolving virtual, then resolve it with only the local
				// pruning point changed.
				if err := tc.ValidateAndInsertBlock(block, false, true); err != nil {
					t.Fatal(err)
				}
				if boundary {
					stagingArea := model.NewStagingArea()
					if err := tc.PruningStore().StagePruningPoint(tc.DatabaseContext(), stagingArea, parent); err != nil {
						t.Fatal(err)
					}
					if err := staging.CommitAllChanges(tc.DatabaseContext(), stagingArea); err != nil {
						t.Fatal(err)
					}
					health, err := tc.UTXOSetHealth()
					if err != nil {
						t.Fatal(err)
					}
					if !health.Checked || !health.BaselineVerified {
						t.Fatalf("the boundary case needs a verified baseline, got %+v", health)
					}
				}

				status, err := tc.ConsensusStateManager().ResolveBlockStatusTest(model.NewStagingArea(),
					consensushashing.BlockHash(block), false)
				if err != nil {
					t.Fatal(err)
				}
				if status != externalapi.StatusDisqualifiedFromChain {
					t.Fatalf("a block spending a missing input resolved %s under the strict gates; want %s",
						status, externalapi.StatusDisqualifiedFromChain)
				}
			})
		}
	})
}
