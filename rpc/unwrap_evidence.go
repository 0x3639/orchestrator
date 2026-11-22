package rpc

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"slices"
	"sync"

	"github.com/ethereum/go-ethereum"
	ecommon "github.com/ethereum/go-ethereum/common"
	etypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"

	"orchestrator/common"
	"orchestrator/common/config"
)

// AgreedUnwrapLog reads the exact source log from every configured endpoint.
// The caller checks the canonical hash at the event height. All endpoints must
// also be able to serve the canonical height at the required confirmation depth.
func (r *EvmRpc) AgreedUnwrapLog(height uint64, block, tx ecommon.Hash, index uint32, confirmations uint64) (etypes.Log, error) {
	return r.agreedBridgeLog(height, block, tx, uint64(index), confirmations, common.UnwrapSigHash, 3)
}

// AgreedRedeemLog checks the receipt or single-block log on each endpoint.
func (r *EvmRpc) AgreedRedeemLog(log etypes.Log, confirmations uint64) (etypes.Log, error) {
	if len(log.Topics) == 0 {
		return etypes.Log{}, errors.New("missing redeem log topic")
	}
	count := 4
	switch log.Topics[0] {
	case common.RegisteredRedeemSigHash, common.RedeemedSigHash:
	case common.RevokedRedeemSigHash:
		count = 2
	default:
		return etypes.Log{}, errors.New("unexpected redeem log topic")
	}
	return r.agreedBridgeLog(log.BlockNumber, log.BlockHash, log.TxHash, uint64(log.Index), confirmations, log.Topics[0], count)
}

func (r *EvmRpc) agreedBridgeLog(height uint64, block, tx ecommon.Hash, index, confirmations uint64, topic ecommon.Hash, topicCount int) (etypes.Log, error) {
	if len(r.Urls.Urls) == 0 || height > math.MaxUint64-confirmations {
		return etypes.Log{}, errors.New("cannot establish bridge log confirmation depth")
	}
	if confirmations > 0 {
		if _, err := r.CanonicalHash(height + confirmations); err != nil {
			return etypes.Log{}, err
		}
	}
	logs := make([]etypes.Log, len(r.Urls.Urls))
	errs := make([]error, len(logs))
	var wg sync.WaitGroup
	for i := range logs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			logs[i], errs[i] = r.readBridgeLog(i, height, block, tx, index, topic, topicCount)
		}(i)
	}
	wg.Wait()
	for i, log := range logs {
		if errs[i] != nil {
			return etypes.Log{}, fmt.Errorf("bridge log evidence from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], r.Urls.Urls[i]))
		}
		if !slices.Equal(log.Topics, logs[0].Topics) || !bytes.Equal(log.Data, logs[0].Data) {
			return etypes.Log{}, errors.New("EVM endpoints disagree on the bridge log payload")
		}
	}
	return logs[0], nil
}

func (r *EvmRpc) readBridgeLog(endpoint int, height uint64, block, tx ecommon.Hash, index uint64, topic ecommon.Hash, topicCount int) (etypes.Log, error) {
	matches := func(log *etypes.Log) bool {
		return log != nil && !log.Removed && log.Address == r.bridgeAddress &&
			log.BlockNumber == height && log.BlockHash == block && log.TxHash == tx && uint64(log.Index) == index &&
			len(log.Topics) == topicCount && log.Topics[0] == topic
	}
	var receipt *etypes.Receipt
	receiptErr := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		var err error
		receipt, err = c.TransactionReceipt(ctx, tx)
		if errors.Is(err, ethereum.NotFound) {
			return nil
		}
		return err
	})
	if receiptErr == nil && receipt != nil && receipt.TxHash == tx && receipt.BlockHash == block && receipt.BlockNumber != nil &&
		receipt.BlockNumber.IsUint64() && receipt.BlockNumber.Uint64() == height && receipt.Status == etypes.ReceiptStatusSuccessful {
		for _, log := range receipt.Logs {
			if matches(log) {
				return *log, nil
			}
		}
	}
	// Pruned and inconsistent receipts can be resolved by querying just the
	// event height. Some providers do not support block-hash log filters.
	number := new(big.Int).SetUint64(height)
	query := ethereum.FilterQuery{FromBlock: number, ToBlock: number, Addresses: []ecommon.Address{r.bridgeAddress}, Topics: [][]ecommon.Hash{{topic}}}
	var logs []etypes.Log
	err := r.callEndpoint(endpoint, func(ctx context.Context, c *ethclient.Client) error {
		var err error
		logs, err = c.FilterLogs(ctx, query)
		return err
	})
	if err != nil {
		return etypes.Log{}, errors.Join(receiptErr, err)
	}
	for i := range logs {
		if matches(&logs[i]) {
			return logs[i], nil
		}
	}
	return etypes.Log{}, errors.Join(receiptErr, errors.New("canonical bridge log unavailable"))
}
