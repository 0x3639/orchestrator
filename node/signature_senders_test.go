package node

import "testing"

func TestSignatureSendPollKeepsSeparateRetryCounts(t *testing.T) {
	var wraps, unwraps map[string]uint32
	poll := newSignatureSendPoll(
		func(counts map[string]uint32) { wraps = counts },
		func(counts map[string]uint32) { unwraps = counts },
	)

	const id = "pending"
	poll()
	wraps[id] = 3
	unwraps[id] = 1
	poll()
	if wraps[id] != 3 || unwraps[id] != 1 {
		t.Fatalf("second poll counts = (%d, %d), want (3, 1)", wraps[id], unwraps[id])
	}

	delete(wraps, id)
	poll()
	if wraps[id] != 0 || unwraps[id] != 1 {
		t.Fatalf("after wrap completion counts = (%d, %d), want (0, 1)", wraps[id], unwraps[id])
	}
}
