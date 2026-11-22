package network

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"

	"orchestrator/common"
)

// An uppercase recipient in the source log is the same Zenon address. The row
// keeps the log's spelling, verifies against its source and signs the message
// for the canonical address.
func TestUppercaseSourceRecipientSignsTheCanonicalMessage(t *testing.T) {
	upper := strings.ToUpper(testRecipient)
	ev := testEvent(100, canonicalHeader(100))
	ev.Token = ecommon.HexToAddress("0x10")
	ev.From = ecommon.HexToAddress("0x22")
	log := transferLog(t, ev, upper, 150)
	provider := &unwrapProvider{}
	provider.set(evidenceReceipt(log), nil, 140)
	net := sourceNetwork(t, provider)
	if err := net.InterpretLog(log, false); err != nil {
		t.Fatal(err)
	}
	rows, err := net.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	// A rollback to the previous build compares this string with the log.
	if rows[0].To != upper {
		t.Fatalf("stored recipient %q, want the log's spelling %q", rows[0].To, upper)
	}
	manager := &NetworksManager{evmNetworks: []*evmNetwork{net}}
	if err := manager.VerifyUnwrapsForSigning(rows); err != nil {
		t.Fatal(err)
	}
	message, err := rows[0].GetMessage()
	if err != nil {
		t.Fatal(err)
	}
	lower := *rows[0]
	lower.To = testRecipient
	canonical, err := lower.GetMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message, canonical) {
		t.Fatal("the uppercase spelling signs a different message")
	}
	if !rows[0].SameTransfer(&lower) {
		t.Fatal("two spellings of one recipient compared as different transfers")
	}
	other := *rows[0]
	other.To = "z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7"
	if rows[0].SameTransfer(&other) {
		t.Fatal("two different recipients compared as the same transfer")
	}
}

// Zenon startup stores a registration with the canonical address before the
// EVM rescan reaches its log. A log that spells the same recipient in
// uppercase is the same transfer and must not stop the rescan.
func TestRescanAcceptsUppercaseLogForAZenonRegisteredRow(t *testing.T) {
	recipient, err := types.ParseAddress(testRecipient)
	if err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		to      string
		wantErr bool
	}{
		"uppercase beneficiary with an affiliate": {strings.ToUpper(testRecipient) + "&" + testRecipient, false},
		"another beneficiary with an affiliate":   {"z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7&" + testRecipient, true},
	} {
		t.Run(name, func(t *testing.T) {
			ev := testEvent(100, canonicalHeader(100))
			ev.Token = ecommon.HexToAddress("0x10")
			ev.From = ecommon.HexToAddress("0x22")
			log := transferLog(t, ev, tc.to, 150)
			provider := &unwrapProvider{}
			provider.set(evidenceReceipt(log), nil, 140)
			net := sourceNetwork(t, provider)
			registered := common.ZnnUnwrapToOrchestatorUnwrap(&definition.UnwrapTokenRequest{
				NetworkClass: net.NetworkClass(), ChainId: net.ChainId(), TransactionHash: types.Hash(ev.TransactionHash),
				LogIndex: ev.LogIndex, ToAddress: recipient, TokenAddress: ev.Token.Hex(), Amount: big.NewInt(150), Signature: "sig", Redeemed: 1,
			})
			if err := net.eventsStore().AddUnwrapRequest(registered); err != nil {
				t.Fatal(err)
			}
			err := net.InterpretLog(log, false)
			if (err != nil) != tc.wantErr {
				t.Fatalf("historical ingestion error %v, want error %v", err, tc.wantErr)
			}
		})
	}
}
