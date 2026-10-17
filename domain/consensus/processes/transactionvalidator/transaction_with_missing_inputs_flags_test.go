package transactionvalidator

import (
	"errors"
	"sync"
	"testing"

	"github.com/cloudflare/circl/sign/mldsa/mldsa44"

	"github.com/HoosatNetwork/HTND/v2/domain/consensus/model/externalapi"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/ruleerrors"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/consensushashing"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/txscript"
	"github.com/HoosatNetwork/HTND/v2/domain/consensus/utils/utxo"
	"github.com/HoosatNetwork/HTND/v2/domain/dagconfig"
	"github.com/HoosatNetwork/HTND/v2/util"
)

// TestMissingInputPathUsesActivatedScriptFlags: once ML-DSA-44 is active, the missing-input path must
// count sigops and run scripts with the flags the normal validator derives from the DAA score
// (scriptFlagsForDAAScore). It used ScriptNoFlags, so a correctly signed ML-DSA-44 found input failed
// with ErrWrongSigOpCount there. The test calls only scriptFlagsForDAAScore and the missing-input
// validator; it does not run the full in-context validator. The controls pin that the flags follow the
// DAA score (before activation the same input still fails) and that an activated ML-DSA-44 signature is
// really checked (a corrupted one fails).
func TestMissingInputPathUsesActivatedScriptFlags(t *testing.T) {
	var seed [mldsa44.SeedSize]byte
	pub, priv := mldsa44.NewKeyFromSeed(&seed)
	addr, err := util.NewAddressPublicKeyHashMLDSA44(pub.Bytes(), util.Bech32PrefixHoosat)
	if err != nil {
		t.Fatal(err)
	}
	spk, err := txscript.PayToAddrScript(addr)
	if err != nil {
		t.Fatal(err)
	}
	tx := &externalapi.DomainTransaction{
		Inputs: []*externalapi.DomainTransactionInput{
			{PreviousOutpoint: externalapi.DomainOutpoint{Index: 0}, Sequence: ^uint64(0), SigOpCount: 1,
				UTXOEntry: utxo.NewUTXOEntry(1000, spk, false, 1)},
			{PreviousOutpoint: externalapi.DomainOutpoint{Index: 1}, Sequence: ^uint64(0), SigOpCount: 1}, // missing
		},
		Outputs: []*externalapi.DomainTransactionOutput{{Value: 500, ScriptPublicKey: spk}},
	}
	sig, err := txscript.SignatureScriptMLDSA44(tx, 0, consensushashing.SigHashAll, priv, &consensushashing.SighashReusedValues{})
	if err != nil {
		t.Fatal(err)
	}
	tx.Inputs[0].SignatureScript = sig

	params := dagconfig.Params{POWScores: []uint64{100}, HardForkGates: dagconfig.HardForkGates{MLDSA44SignaturesBlockVersion: 2}}
	v := transactionValidator{dagParams: &params, mldsa44Cache: txscript.NewMLDSA44Cache(10, 10),
		enginePool: &sync.Pool{New: func() any { return &txscript.Engine{} }}}

	if flags := v.scriptFlagsForDAAScore(1000); flags&txscript.ScriptEnableMLDSA44 == 0 {
		t.Fatal("precondition: ML-DSA-44 should be active at DAA 1000")
	}
	if err := v.ValidateTransactionWithMissingInputsAndPopulateFee(nil, tx, nil, 1000); err != nil {
		t.Fatalf("missing-input path rejects a valid ML-DSA-44 input after activation: %v", err)
	}

	// Control, before activation: no ML-DSA-44 flag, so the declared sigop does not count.
	if flags := v.scriptFlagsForDAAScore(50); flags&txscript.ScriptEnableMLDSA44 != 0 {
		t.Fatal("precondition: ML-DSA-44 should be inactive at DAA 50")
	}
	if err := v.ValidateTransactionWithMissingInputsAndPopulateFee(nil, tx, nil, 50); !errors.Is(err, ruleerrors.ErrWrongSigOpCount) {
		t.Fatalf("before activation the ML-DSA-44 sigop must not count; got %v", err)
	}

	// Control, after activation: a corrupted signature is rejected.
	corrupted := tx.Clone()
	corrupted.Inputs[0].SignatureScript[len(corrupted.Inputs[0].SignatureScript)/2] ^= 0xff
	if err := v.ValidateTransactionWithMissingInputsAndPopulateFee(nil, corrupted, nil, 1000); err == nil {
		t.Fatal("missing-input path accepted a corrupted ML-DSA-44 signature after activation")
	}
}
