package network

import (
	"errors"
	"fmt"
	"testing"

	"github.com/zenon-network/go-zenon/vm/constants"
)

func TestBridgeObjectGone(t *testing.T) {
	gone := []error{
		constants.ErrDataNonExistent,
		constants.ErrUnknownNetwork,
		constants.ErrTokenNotFound,
		// RPC errors arrive as fresh values carrying only the message.
		errors.New("unknown network"),
		errors.New("data non existent"),
		fmt.Errorf("wrapped: %w", constants.ErrUnknownNetwork),
	}
	for _, err := range gone {
		if !bridgeObjectGone(err) {
			t.Fatalf("%v should be treated as gone", err)
		}
	}
	notGone := []error{
		nil,
		errors.New("connection refused"),
		errors.New("i/o timeout"),
		constants.ErrBridgeHalted,
	}
	for _, err := range notGone {
		if bridgeObjectGone(err) {
			t.Fatalf("%v must not be treated as gone", err)
		}
	}
}
