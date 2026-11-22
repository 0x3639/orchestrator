package network

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/rpc/api"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
)

func TestBridgeListenerReplaysFailedReceiveInOrder(t *testing.T) {
	first, missed, last := startupReceive(2), startupReceive(3), startupReceive(4)
	for _, failure := range []string{"lookup", "interpretation", "missing height", "unconfirmed"} {
		t.Run(failure, func(t *testing.T) {
			recovered := false
			ledger := &startupLedger{page: func(height uint64) *api.AccountBlockListMarshal {
				switch height {
				case 2:
					if failure == "missing height" {
						return startupPage(first, last)
					}
					if failure == "unconfirmed" {
						pending := *missed
						pending.ConfirmationDetail = nil
						return startupPage(first, &pending, last)
					}
					return startupPage(first, missed, last)
				case 3:
					return startupPage(missed, last)
				default:
					return startupPage()
				}
			}, lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
				for _, b := range []*api.AccountBlockMarshal{first, missed, last} {
					if b.Hash == hash {
						return b, nil
					}
					if b.FromBlockHash == hash {
						send := startupSend(b)
						if b == missed && !recovered {
							switch failure {
							case "lookup":
								return nil, errors.New("temporary RPC failure")
							case "interpretation":
								send.Data = definition.ABIBridge.Methods[definition.WrapTokenMethodName].Id()
							}
						}
						return send, nil
					}
				}
				return nil, errors.New("unexpected lookup")
			}}
			n := startupNetwork(t, ledger)
			height, _, err := n.processBridgeAccountPage(context.Background(), 1, n.refreshSettings)
			if height != 2 || err == nil {
				t.Fatalf("failed page returned height %d, error %v", height, err)
			}
			checkStartupCursor(t, n, 2)
			for _, hash := range ledger.lookups {
				if hash == last.FromBlockHash {
					t.Fatal("later receive reached interpreter before the missed one")
				}
			}
			recovered = true
			ledger.lookups = nil
			// A new listener reads its position back from the existing database.
			resumed := startupNetwork(t, ledger)
			resumed.dbManager = n.dbManager
			if err := resumed.Sync(); err != nil {
				t.Fatal(err)
			}
			want := []types.Hash{missed.Hash, missed.FromBlockHash, last.Hash, last.FromBlockHash}
			if !reflect.DeepEqual(ledger.lookups, want) {
				t.Fatalf("restart read %v, want %v", ledger.lookups, want)
			}
			checkStartupCursor(t, resumed, 4)
		})
	}
}

func TestBridgeListenerRetriesFromSuccessfulPrefix(t *testing.T) {
	first, second := startupReceive(2), startupReceive(3)
	available := false
	l := &startupLedger{
		page: func(height uint64) *api.AccountBlockListMarshal {
			if height == 2 {
				return startupPage(first, second)
			}
			return startupPage(second)
		},
		lookup: func(hash types.Hash) (*api.AccountBlockMarshal, error) {
			if hash == first.FromBlockHash {
				return startupSend(first), nil
			}
			if !available {
				return nil, nil
			}
			return startupSend(second), nil
		},
	}
	n := startupNetwork(t, l)
	height, _, err := n.processBridgeAccountPage(context.Background(), 1, n.refreshSettings)
	if err == nil || height != 2 {
		t.Fatalf("missing send: height %d, error %v", height, err)
	}
	available = true
	height, _, err = n.processBridgeAccountPage(context.Background(), height, n.refreshSettings)
	if err != nil || height != 3 {
		t.Fatalf("retry: height %d, error %v", height, err)
	}
	checkStartupCursor(t, n, 3)
}

func TestBridgeListenerCancelsWhileAwaitingConfirmation(t *testing.T) {
	waiting := make(chan struct{})
	receive := startupReceive(2)
	receive.ConfirmationDetail = nil
	n := startupNetwork(t, &startupLedger{page: func(uint64) *api.AccountBlockListMarshal {
		select {
		case <-waiting:
		default:
			close(waiting)
		}
		return startupPage(receive)
	}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		n.followBridgeAccount(ctx, nil, nil, n.refreshSettings)
	}()
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("listener never read the unconfirmed receive")
	}
	cancel()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("listener remained blocked after cancellation")
	}
	checkStartupCursor(t, n, 1)
}
