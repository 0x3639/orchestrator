package node

import (
	"fmt"

	"github.com/zenon-network/go-zenon/common/types"
	"orchestrator/db"
)

// wrapNeedsSignature re-reads the stored wrap request right before it is
// added to a signing pool. A request is signed again only when it has no
// signature yet or is marked for resigning; a signature that exists but was
// not sent yet is kept. A read error, or a record that is missing although the
// caller just read or stored it, is returned as an error so the signing round
// stops instead of acting on a record it cannot see.
func wrapNeedsSignature(store db.ZnnStorage, id types.Hash) (bool, error) {
	local, err := store.GetWrapRequestById(id)
	if err != nil {
		return false, err
	}
	if local == nil {
		return false, fmt.Errorf("wrap request %s missing from local storage", id)
	}
	resign, err := store.GetResignStatus(id)
	if err != nil {
		return false, err
	}
	// on a new key sign, this will be set as unsigned if the signature set will not be succeeded
	return resign || len(local.Signature) == 0, nil
}
