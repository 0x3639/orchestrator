package unwrap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"

	"orchestrator/db"
)

// A stored checkpoint of any length other than the 8 bytes of earlier
// builds or the 16 bytes SetScanHeights writes must surface as a typed error,
// request a stop and stay on disk.
func TestEvmCursorRejectsWrongLength(t *testing.T) {
	for _, length := range []int{0, 7, 8, 9, 15, 16, 17} {
		mem := zdb.NewMemDB()
		stop := make(chan os.Signal, 1)
		store := NewEvmStorage(mem, stop, 100)
		raw := make([]byte, length)
		for i := range raw {
			raw[i] = byte(i + 1)
		}
		if length == 16 {
			// A valid pair keeps the redeem checkpoint at or below the scan height.
			raw = binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 50), 40)
		}
		if err := mem.Put(getLastUpdateKey(), raw); err != nil {
			t.Fatal(err)
		}

		height, err := store.GetLastUpdateHeight()
		if length == 8 || length == 16 {
			if err != nil || height != binary.LittleEndian.Uint64(raw) {
				t.Fatalf("length %d: height %d, err %v", length, height, err)
			}
			if err := store.SetLastUpdateHeight(42); err != nil {
				t.Fatal(err)
			}
			if h, err := store.GetLastUpdateHeight(); err != nil || h != 42 {
				t.Fatalf("length %d rewrite: height %d, err %v", length, h, err)
			}
			if len(stop) != 0 {
				t.Fatal("valid cursor requested a stop")
			}
			continue
		}
		var corrupt *db.CorruptCursorError
		if !errors.Is(err, db.ErrCorruptCursor) || !errors.As(err, &corrupt) || height != 0 {
			t.Fatalf("length %d: height %d, err %v", length, height, err)
		}
		if corrupt.Got != length || corrupt.Want != 16 || !strings.Contains(err.Error(), "EVM scan checkpoint (key 0x01)") {
			t.Fatalf("length %d: %+v / %q", length, corrupt, err)
		}
		if len(stop) != 1 {
			t.Fatalf("length %d: no stop requested", length)
		}
		<-stop
		if err := store.SetLastUpdateHeight(42); !errors.Is(err, db.ErrCorruptCursor) {
			t.Fatalf("length %d: overwrite of corrupt cursor returned %v", length, err)
		}
		if got, err := mem.Get(getLastUpdateKey()); err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("length %d: stored cursor changed to %x (%v)", length, got, err)
		}
	}
}

// A pair whose redeem checkpoint is above its scan height cannot have been
// written by SetScanHeights and is reported as corrupt.
func TestEvmCursorRejectsRedeemAboveScan(t *testing.T) {
	mem := zdb.NewMemDB()
	stop := make(chan os.Signal, 1)
	store := NewEvmStorage(mem, stop, 100)
	raw := binary.LittleEndian.AppendUint64(binary.LittleEndian.AppendUint64(nil, 40), 50)
	if err := mem.Put(getLastUpdateKey(), raw); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := store.GetScanHeights(0); !errors.Is(err, db.ErrCorruptCursor) {
		t.Fatalf("err %v", err)
	}
	if len(stop) != 1 {
		t.Fatal("no stop requested")
	}
}

func TestEvmCursorMissingUsesDeploymentHeight(t *testing.T) {
	store := NewEvmStorage(zdb.NewMemDB(), nil, 100)
	if h, err := store.GetLastUpdateHeight(); err != nil || h != 100 {
		t.Fatalf("height %d, err %v", h, err)
	}
}
