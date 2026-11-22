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
	if len(r.Urls.Urls) == 0 || height > math.MaxUint64-confirmations {
		return etypes.Log{}, errors.New("cannot establish unwrap confirmation depth")
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
			logs[i], errs[i] = r.readUnwrapLog(i, height, block, tx, index)
		}(i)
	}
	wg.Wait()
	for i, log := range logs {
		if errs[i] != nil {
			return etypes.Log{}, fmt.Errorf("unwrap evidence from endpoint #%d: %s", i, config.RedactErrorForURL(errs[i], r.Urls.Urls[i]))
		}
		if !slices.Equal(log.Topics, logs[0].Topics) || !bytes.Equal(log.Data, logs[0].Data) {
			return etypes.Log{}, errors.New("EVM endpoints disagree on the unwrap log payload")
		}
	}
	return logs[0], nil
}

func (r *EvmRpc) readUnwrapLog(endpoint int, height uint64, block, tx ecommon.Hash, index uint32) (etypes.Log, error) {
	matches := func(log *etypes.Log) bool {
		return log != nil && !log.Removed && log.Address == r.bridgeAddress &&
			log.BlockNumber == height && log.BlockHash == block && log.TxHash == tx && uint64(log.Index) == uint64(index) &&
			len(log.Topics) == 3 && log.Topics[0] == common.UnwrapSigHash
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
	query := ethereum.FilterQuery{FromBlock: number, ToBlock: number, Addresses: []ecommon.Address{r.bridgeAddress}, Topics: [][]ecommon.Hash{{common.UnwrapSigHash}}}
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
	return etypes.Log{}, errors.Join(receiptErr, errors.New("canonical unwrap log unavailable"))
}
