package network

import (
	"errors"
	"github.com/zenon-network/go-zenon/rpc/api/embedded"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"orchestrator/common/events"
	"orchestrator/health/snapshot"
)

// The initial scan and live bridge-account subscription run in sequence. Account
// heights include send blocks, which advance continuity but are not receive work.
type receiveCoverage struct {
	lastBlock, lastReceive uint64
	partial                bool
}

func (c *receiveCoverage) start(s *snapshot.Store, cursor uint64, verified bool) {
	if s == nil {
		return
	}
	*c = receiveCoverage{lastBlock: cursor}
	// The legacy cursor records a high-water mark, not proof of contiguous coverage.
	c.partial = cursor > 1 && !verified
	if cursor > 1 {
		c.lastReceive = cursor
	}
	c.publish(s)
}

func (c *receiveCoverage) block(s *snapshot.Store, height uint64, receive, processed bool) {
	if s == nil || height <= c.lastBlock {
		return
	}
	if height != c.lastBlock+1 || !processed {
		c.partial = true
	}
	c.lastBlock = height
	if receive && processed {
		c.lastReceive = height
	}
	c.publish(s)
}

func (c *receiveCoverage) publish(s *snapshot.Store) {
	s.UpdateZenon(func(z *snapshot.Zenon) {
		z.ProcessedReceiveHeight = snapshot.Measure(c.lastReceive)
		if c.partial {
			z.ProcessedReceiveHeight.Quality = "partial"
		}
	})
}

// The Zenon RPC returns a globally paginated list, then the signer filters by finality.
// An incomplete page also makes a zero for an absent chain partial.
func filterWrapPage(store *snapshot.Store, page *embedded.WrapTokenRequestList, err error) ([]*definition.WrapTokenRequest, error) {
	type counts struct {
		unsigned, confirming int
		ids                  []snapshot.PendingID
	}
	var eligible []*definition.WrapTokenRequest
	var byChain map[uint32]counts
	if store != nil {
		byChain = make(map[uint32]counts)
	}
	if err == nil && page != nil {
		if len(page.List) > 0 {
			eligible = make([]*definition.WrapTokenRequest, 0)
		}
		for _, item := range page.List {
			if item == nil || item.WrapTokenRequest == nil {
				err = errors.New("missing request in unsigned wrap page")
				break
			}
			if item.ConfirmationsToFinality == 0 {
				eligible = append(eligible, item.WrapTokenRequest)
			}
			if store == nil {
				continue
			}
			c := byChain[item.ChainId]
			if item.ConfirmationsToFinality == 0 {
				c.unsigned++
				if len(c.ids) < snapshot.AgeSampleLimit {
					c.ids = append(c.ids, snapshot.PendingID{Hash: item.Id})
				}
			} else {
				c.confirming++
			}
			byChain[item.ChainId] = c
		}
	}
	store.UpdateNetworks(func(n *snapshot.Network) {
		if err != nil || page == nil {
			n.Wrap.AwaitingFinality = snapshot.Unknown[int]()
			n.Wrap.ObserveUnsigned(snapshot.Unknown[int](), nil)
			return
		}
		c := byChain[n.ChainID]
		unsigned, confirming := snapshot.Measure(c.unsigned), snapshot.Measure(c.confirming)
		if page.Count != len(page.List) {
			unsigned.Quality = "partial"
			confirming.Quality = "partial"
		}
		n.Wrap.AwaitingFinality = confirming
		n.Wrap.ObserveUnsigned(unsigned, c.ids)
	})
	if err != nil {
		return nil, err
	}
	return eligible, nil
}

func observeUnwraps(store *snapshot.Store, chain uint32, requests []*events.UnwrapRequestEvm, err error) {
	if store == nil {
		return
	}
	sample := requests[:min(len(requests), snapshot.AgeSampleLimit)]
	ids := make([]snapshot.PendingID, 0, len(sample))
	for _, item := range sample {
		ids = append(ids, snapshot.PendingID{Hash: item.TransactionHash, Index: item.LogIndex})
	}
	store.UpdateNetwork(chain, func(n *snapshot.Network) { n.Unwrap.ObserveUnsigned(snapshot.Reading(len(requests), err), ids) })
}
