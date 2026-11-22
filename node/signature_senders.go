package node

import "sync"

// Each sender owns its retry counts across successive polls. A poll waits for
// both senders before the next poll can use those counts.
func newSignatureSendPoll(sendWraps, sendUnwraps func(map[string]uint32)) func() {
	wrapCounts := make(map[string]uint32)
	unwrapCounts := make(map[string]uint32)
	return func() {
		var senders sync.WaitGroup
		senders.Add(2)
		go func() {
			defer senders.Done()
			sendWraps(wrapCounts)
		}()
		go func() {
			defer senders.Done()
			sendUnwraps(unwrapCounts)
		}()
		senders.Wait()
	}
}
