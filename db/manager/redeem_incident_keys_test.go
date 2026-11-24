package manager

import (
	"encoding/binary"
	"encoding/json"
	"os"
	"testing"

	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"

	"orchestrator/common"
	"orchestrator/common/storage"
)

// Opening the databases moves incident evidence that an earlier build wrote
// under byte 4, so the incident still holds the bridge, and an open fails
// when such evidence cannot be accounted for.
func TestOpeningMovesRedeemIncidentsOfAnEarlierBuild(t *testing.T) {
	ev := common.RedeemIncident{ChainID: 1, Log: etypes.Log{BlockNumber: 9, BlockHash: ecommon.HexToHash("0xa4"),
		TxHash: ecommon.HexToHash("0xd3"), Index: 2, Topics: []ecommon.Hash{common.RegisteredRedeemSigHash}, Data: []byte{0xb0}}}
	key := binary.BigEndian.AppendUint32([]byte{4}, ev.ChainID)
	key = append(key, ev.Log.BlockHash[:]...)
	key = append(key, ev.Log.TxHash[:]...)
	key = binary.BigEndian.AppendUint64(key, uint64(ev.Log.Index))
	data, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		value   []byte
		wantErr bool
	}{{"evidence of an earlier build", data, false}, {"unreadable evidence", []byte("not json"), true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			ldb, err := storage.CreateOrOpenLevelDb(common.ZenonNetworkName)
			if err != nil {
				t.Fatal(err)
			}
			if err := ldb.Put(key, tc.value, nil); err != nil {
				t.Fatal(err)
			}
			if err := ldb.Close(); err != nil {
				t.Fatal(err)
			}
			m, err := NewDbManager(make(chan os.Signal, 1))
			if tc.wantErr {
				if err == nil {
					t.Fatal("opened a database with evidence that cannot be accounted for")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if pending, err := m.RedeemIncidents().HasPendingRedeemIncident(); err != nil || !pending {
				t.Fatalf("the incident of the earlier build does not hold the bridge: %t %v", pending, err)
			}
		})
	}
}
