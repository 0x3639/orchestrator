package network

import (
	"errors"

	"github.com/zenon-network/go-zenon/vm/constants"
)

// bridgeObjectGone reports whether an embedded-bridge RPC error means the
// object being looked up cannot exist in the bridge's current state: the
// request itself is gone, or the network or token pair it refers to is no
// longer registered. The bridge validates a request's network and token pair
// against current state when it is looked up, so a historical request for a
// network that has since been removed answers "unknown network" rather than
// "data non existent". None of these can be signed, redeemed or revoked, so
// the sync records nothing and moves on instead of aborting.
func bridgeObjectGone(err error) bool {
	if err == nil {
		return false
	}
	for _, known := range []error{constants.ErrDataNonExistent, constants.ErrUnknownNetwork, constants.ErrTokenNotFound} {
		if errors.Is(err, known) || err.Error() == known.Error() {
			return true
		}
	}
	return false
}
