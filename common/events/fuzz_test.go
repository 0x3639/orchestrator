package events

import (
	"math/big"
	"strings"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	"github.com/zenon-network/go-zenon/common/types"
)

func FuzzDeserializeEvmUnwrapRequest(f *testing.F) {
	valid := &UnwrapRequestEvm{
		NetworkClass:    2,
		ChainId:         1,
		BlockNumber:     21000000,
		BlockHash:       ecommon.HexToHash("0x1111"),
		TransactionHash: ecommon.HexToHash("0x2222"),
		LogIndex:        7,
		From:            ecommon.HexToAddress("0x3333"),
		To:              types.ZnnTokenStandard.String(),
		Token:           ecommon.HexToAddress("0x4444"),
		Amount:          big.NewInt(123456789),
	}
	if b, err := valid.Serialize(); err == nil {
		f.Add(b)
	}
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0xff, 0xff})
	f.Add([]byte{0x0a, 0x01, 0x00})

	f.Fuzz(func(t *testing.T, data []byte) {
		ev, err := DeserializeEvmUnwrapRequest(data)
		if err != nil {
			return
		}
		// Round-trip serialization must never panic and must stay consistent.
		if ev.Amount != nil && !ev.SameTransfer(ev) {
			t.Fatalf("SameTransfer must be reflexive for non-nil amount")
		}
		_, _ = ev.GetMessage()
		roundTrip, err := ev.Serialize()
		if err != nil {
			t.Fatalf("Serialize after Deserialize failed: %v", err)
		}
		ev2, err := DeserializeEvmUnwrapRequest(roundTrip)
		if err != nil {
			t.Fatalf("second Deserialize failed: %v", err)
		}
		if ev.Amount != nil && ev2.Amount != nil && !ev.SameTransfer(ev2) {
			t.Fatalf("round-trip changed transfer identity")
		}
	})
}

func FuzzDeserializeWrapEventZnn(f *testing.F) {
	valid := &WrapRequestZnn{
		NetworkClass: 2,
		ChainId:      1,
		Id:           types.HexToHashPanic("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
		ToAddress:    "0x000000000000000000000000000000000000dEaD",
		TokenAddress: "0x000000000000000000000000000000000000bEEF",
		Amount:       big.NewInt(1000),
		Fee:          big.NewInt(10),
	}
	if b, err := valid.Serialize(); err == nil {
		f.Add(b)
	}
	f.Add([]byte{})
	// Corrupted protobuf with short Id bytes (field 3 length=1) — tests panic safety!
	f.Add([]byte{0x1a, 0x01, 0xab})
	f.Add([]byte{0x08, 0x02, 0x10, 0x01})

	f.Fuzz(func(t *testing.T, data []byte) {
		ev, err := DeserializeWrapEventZnn(data)
		if err != nil {
			return
		}
		if _, err := ev.Serialize(); err != nil {
			t.Fatalf("Serialize after Deserialize failed: %v", err)
		}
	})
}

func TestSameTransferAdversarialRecipientsAndAmounts(t *testing.T) {
	addr := types.ParseAddressPanic("z1qqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqqsggv2f")
	base := &UnwrapRequestEvm{
		NetworkClass:    2,
		ChainId:         1,
		TransactionHash: ecommon.HexToHash("0xabc"),
		LogIndex:        3,
		To:              addr.String(),
		Token:           ecommon.HexToAddress("0xbeef"),
		Amount:          big.NewInt(500),
	}

	// Uppercase bech32 address from EVM log must match canonical lowercase Zenon address.
	upper := *base
	upper.To = strings.ToUpper(addr.String())
	if !base.SameTransfer(&upper) || !upper.SameTransfer(base) {
		t.Fatal("SameTransfer failed to match uppercase bech32 recipient")
	}

	// Mismatched amount, nil amount, or tampered recipient must NEVER match.
	nilAmt := *base
	nilAmt.Amount = nil
	if base.SameTransfer(&nilAmt) || nilAmt.SameTransfer(base) {
		t.Fatal("nil amount matched non-nil amount")
	}

	badTo := *base
	badTo.To = "not-a-zenon-address"
	if base.SameTransfer(&badTo) {
		t.Fatal("invalid recipient matched valid recipient")
	}
	if _, err := badTo.GetMessage(); err == nil {
		t.Fatal("GetMessage accepted invalid Zenon recipient")
	}
	if _, err := nilAmt.Hash(); err == nil {
		t.Fatal("Hash accepted nil amount")
	}

	// Regression: corrupted/truncated wrap protobuf bytes must return an error, never panic.
	for _, bad := range [][]byte{{}, {0x1a, 0x01, 0xab}, {0x08, 0x02, 0x10, 0x01}} {
		if _, err := DeserializeWrapEventZnn(bad); err == nil {
			t.Fatalf("DeserializeWrapEventZnn accepted truncated Id bytes %x", bad)
		}
	}
	// Regression: nil Amount/Fee in WrapRequestZnn must serialize safely without panic.
	nilWrap := &WrapRequestZnn{
		NetworkClass: 2,
		ChainId:      1,
		Id:           types.HexToHashPanic("0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20"),
	}
	if _, err := nilWrap.Serialize(); err != nil {
		t.Fatalf("Serialize with nil Amount/Fee failed: %v", err)
	}
}
