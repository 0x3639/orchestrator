package node

import (
	"testing"

	"github.com/syndtr/goleveldb/leveldb"

	"orchestrator/common"
	"orchestrator/common/config"
	"orchestrator/db/wrap"
)

func TestRestartAfterRedeemAcknowledgment(t *testing.T) {
	for _, tc := range []struct {
		name           string
		before, during bool
		want           uint8
	}{
		{"redeem only", false, false, common.HaltedState},
		{"preexisting emergency", true, false, common.EmergencyState},
		{"independent emergency during halt", false, true, common.EmergencyState},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, dbDir := t.TempDir(), t.TempDir()
			cfg := config.Config{DataPath: dir}
			if tc.before {
				cfg.GlobalState = common.EmergencyState
			}
			open := func() (*Node, *leveldb.DB) {
				t.Helper()
				db, err := leveldb.OpenFile(dbDir, nil)
				if err != nil {
					t.Fatal(err)
				}
				n := &Node{config: &cfg, state: common.NewPersistentGlobalState(&cfg.GlobalState, func(state uint8) error {
					return config.WriteGlobalState(dir, state)
				})}
				if err := n.state.RestoreRedeemIncidents(wrap.NewRedeemIncidentStore(db)); err != nil {
					t.Fatal(err)
				}
				return n, db
			}
			if err := cfg.InitializeLocked(); err != nil {
				t.Fatal(err)
			}
			n, db := open()
			if err := n.state.ReportRedeemIncident(common.RedeemIncident{ChainID: 1}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Startup restores a pending incident and writes config.json.
			n, db = open()
			if state, err := n.state.GetState(); err != nil || state != common.EmergencyState {
				t.Fatalf("pending state %v: %v", state, err)
			}
			if tc.during {
				if err := n.state.SetState(common.EmergencyState); err != nil {
					t.Fatal(err)
				}
			}
			if err := n.SaveConfig(); err != nil {
				t.Fatal(err)
			}
			if err := n.state.CompleteEmergencyHalt(n.state.RedeemHaltGeneration(), map[uint32]bool{1: true}); err != nil {
				t.Fatal(err)
			}
			if err := db.Close(); err != nil {
				t.Fatal(err)
			}
			// Simulate process loss before the next config write.
			cfg = config.Config{DataPath: dir}
			if found, err := config.ReadConfigFile(&cfg); err != nil || !found {
				t.Fatalf("read config: %t %v", found, err)
			}
			n, db = open()
			defer db.Close()
			if state, err := n.state.GetState(); err != nil || state != tc.want || n.state.RedeemHaltGeneration() != 0 {
				t.Fatalf("restart state=%d pending=%d err=%v", state, n.state.RedeemHaltGeneration(), err)
			}
		})
	}
}
