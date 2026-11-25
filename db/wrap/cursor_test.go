package wrap

import (
	"bytes"
	"encoding/binary"
	"errors"
	"os"
	"strings"
	"testing"

	zdb "github.com/zenon-network/go-zenon/common/db"
	"github.com/zenon-network/go-zenon/common/types"

	"orchestrator/db"
)

func patterned(length int) []byte {
	raw := make([]byte, length)
	for i := range raw {
		raw[i] = byte(i + 1)
	}
	return raw
}

func checkCorrupt(t *testing.T, err error, stop chan os.Signal, key string, got, want int) {
	t.Helper()
	var corrupt *db.CorruptCursorError
	if !errors.Is(err, db.ErrCorruptCursor) || !errors.As(err, &corrupt) {
		t.Fatalf("length %d: err %v", got, err)
	}
	if corrupt.Got != got || corrupt.Want != want || !strings.Contains(err.Error(), key) {
		t.Fatalf("length %d: %+v / %q", got, corrupt, err)
	}
	if len(stop) != 1 {
		t.Fatalf("length %d: no stop requested", got)
	}
	<-stop
}

func TestZnnCursorRejectsWrongLength(t *testing.T) {
	for _, length := range []int{0, 7, 8, 9} {
		mem := zdb.NewMemDB()
		stop := make(chan os.Signal, 1)
		store := NewZnnStorage(mem, stop)
		raw := patterned(length)
		if err := mem.Put(getLastUpdateKey(), raw); err != nil {
			t.Fatal(err)
		}

		height, err := store.GetLastUpdateHeight()
		if length == 8 {
			if err != nil || height != binary.LittleEndian.Uint64(raw) {
				t.Fatalf("length 8: height %d, err %v", height, err)
			}
			if err := store.SetLastUpdateHeight(42); err != nil {
				t.Fatal(err)
			}
			if h, err := store.GetLastUpdateHeight(); err != nil || h != 42 {
				t.Fatalf("length 8 rewrite: height %d, err %v", h, err)
			}
			if len(stop) != 0 {
				t.Fatal("valid cursor requested a stop")
			}
			continue
		}
		if height != 0 {
			t.Fatalf("length %d: height %d", length, height)
		}
		checkCorrupt(t, err, stop, "Zenon sync cursor lastUpdateHeight (key 0x01)", length, 8)
		if err := store.SetLastUpdateHeight(42); !errors.Is(err, db.ErrCorruptCursor) {
			t.Fatalf("length %d: overwrite of corrupt cursor returned %v", length, err)
		}
		if got, err := mem.Get(getLastUpdateKey()); err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("length %d: stored cursor changed to %x (%v)", length, got, err)
		}
	}
}

// The coverage checkpoint is advisory: a wrong length means partial coverage,
// never a panic, and it can never satisfy a cursor match.
func TestReceiveCoverageCheckpointRejectsWrongLength(t *testing.T) {
	for _, length := range []int{0, 7, 8, 9} {
		mem := zdb.NewMemDB()
		store := NewZnnStorage(mem, nil)
		raw := patterned(length)
		if err := mem.Put(receiveCoverageKey, raw); err != nil {
			t.Fatal(err)
		}
		var height uint64 = 0x0807060504030201
		if got := store.HasReceiveCoverageCheckpoint(height); got != (length == 8) {
			t.Fatalf("length %d: checkpoint match %v", length, got)
		}
		if length == 9 && store.HasReceiveCoverageCheckpoint(binary.LittleEndian.Uint64(raw[:8])) {
			t.Fatal("trailing byte ignored")
		}
	}
}

func TestResignStatusRejectsWrongLength(t *testing.T) {
	id := types.Hash{7}
	for _, length := range []int{0, 3, 4, 5} {
		mem := zdb.NewMemDB()
		stop := make(chan os.Signal, 1)
		store := NewZnnStorage(mem, stop)
		raw := patterned(length)
		if err := mem.Put(getResignedStatusKey(id), raw); err != nil {
			t.Fatal(err)
		}
		status, err := store.GetResignStatus(id)
		if length == 4 {
			if err != nil || !status {
				t.Fatalf("length 4: status %v, err %v", status, err)
			}
			continue
		}
		if status {
			t.Fatalf("length %d: reported resignable", length)
		}
		checkCorrupt(t, err, stop, "Zenon wrap resign status for "+id.String(), length, 4)
		if got, err := mem.Get(getResignedStatusKey(id)); err != nil || !bytes.Equal(got, raw) {
			t.Fatalf("length %d: stored status changed to %x (%v)", length, got, err)
		}
	}
}
