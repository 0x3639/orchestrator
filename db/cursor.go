package db

import (
	"errors"
	"fmt"
)

// ErrCorruptCursor reports a stored fixed-width value whose length is not
// the width its writer always produces. Disk damage or a bad restore of the
// events directory can leave such a value behind. Match it with errors.Is;
// the wrapping CorruptCursorError names the key and the observed length.
var ErrCorruptCursor = errors.New("corrupt local sync cursor")

// CorruptCursorError is returned instead of decoding a value of the wrong
// length. The bad value is never overwritten or deleted, so an operator can
// inspect it and recover from a checkpoint.
type CorruptCursorError struct {
	// Key names the value, including its store and raw key bytes.
	Key  string
	Got  int
	Want int
}

func (e *CorruptCursorError) Error() string {
	return fmt.Sprintf("%v: %s holds %d bytes, want %d. The stored value was left unchanged; "+
		"keep the orchestrator stopped and restore a known-good local sync checkpoint "+
		"(operator guide: Local sync checkpoints)", ErrCorruptCursor, e.Key, e.Got, e.Want)
}

func (e *CorruptCursorError) Unwrap() error { return ErrCorruptCursor }

// CheckFixedWidth returns a *CorruptCursorError unless data is exactly want
// bytes long. Call it before every binary.*Endian.Uint* decode of a stored
// value; those decoders panic on short input and ignore trailing bytes.
func CheckFixedWidth(key string, data []byte, want int) error {
	if len(data) != want {
		return &CorruptCursorError{Key: key, Got: len(data), Want: want}
	}
	return nil
}
