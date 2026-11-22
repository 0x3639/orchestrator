package network

import (
	"bytes"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
)

// An uppercase recipient in the source log is the same Zenon address. The
// signer stores the canonical spelling, which is what Zenon registers.
func TestUppercaseSourceRecipientIsStoredCanonically(t *testing.T) {
	upper := strings.ToUpper(testRecipient)
	for name, to := range map[string]string{
		"beneficiary":               upper,
		"beneficiary and affiliate": upper + "&" + upper,
	} {
		t.Run(name, func(t *testing.T) {
			ev := testEvent(100, canonicalHeader(100))
			ev.Token = ecommon.HexToAddress("0x10")
			ev.From = ecommon.HexToAddress("0x22")
			log := transferLog(t, ev, to, 150)
			provider := &unwrapProvider{}
			provider.set(evidenceReceipt(log), nil, 140)
			net := sourceNetwork(t, provider)
			if err := net.InterpretLog(log, false); err != nil {
				t.Fatal(err)
			}
			rows, err := net.eventsStore().GetUnsignedUnwrapRequests()
			if err != nil || len(rows) == 0 {
				t.Fatalf("rows=%d err=%v", len(rows), err)
			}
			for _, row := range rows {
				if row.To != testRecipient {
					t.Fatalf("stored recipient %q, want the canonical %q", row.To, testRecipient)
				}
			}
			manager := &NetworksManager{evmNetworks: []*evmNetwork{net}}
			if err := manager.VerifyUnwrapsForSigning(rows); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Earlier binaries stored the recipient as the log spelled it. Such a row must
// still match its source transfer, and it signs the same message.
func TestStoredUppercaseRecipientStillMatchesItsSource(t *testing.T) {
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
	canonical, err := rows[0].GetMessage()
	if err != nil {
		t.Fatal(err)
	}
	legacy := *rows[0]
	legacy.To = upper
	if err := net.eventsStore().AddUnwrapRequest(legacy); err != nil {
		t.Fatal(err)
	}
	rows, err = net.eventsStore().GetUnsignedUnwrapRequests()
	if err != nil || len(rows) != 1 || rows[0].To != upper {
		t.Fatalf("legacy row was not stored as written: rows=%d err=%v", len(rows), err)
	}
	manager := &NetworksManager{evmNetworks: []*evmNetwork{net}}
	if err := manager.VerifyUnwrapsForSigning(rows); err != nil {
		t.Fatalf("a row with the uppercase spelling no longer matches its source: %v", err)
	}
	message, err := rows[0].GetMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(message, canonical) {
		t.Fatal("the uppercase spelling signs a different message")
	}

	// A different address is still a different transfer.
	other := *rows[0]
	other.To = "z1qpp8g8v4hrd5ernfrgsn86jmv09ra6zrvhvmr7"
	if rows[0].SameTransfer(&other) {
		t.Fatal("two different recipients compared as the same transfer")
	}
}
