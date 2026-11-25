package common

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/pkg/errors"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/vm/constants"
	"github.com/zenon-network/go-zenon/vm/embedded/definition"
	"math/big"
	"orchestrator/common/events"
	"os"
	"os/user"
	"path/filepath"
	"runtime"
)

func ParseAddressString(address string, networkClass uint32) (interface{}, error) {
	switch networkClass {
	case definition.EvmClass:
		if common.IsHexAddress(address) {
			return common.HexToAddress(address), nil
		}
		return nil, errors.New("not a valid evm address")
	case definition.NoMClass:
		return types.ParseAddress(address)
	}

	return nil, errors.New("config not supported")
}

func homeDir() string {
	if home := os.Getenv("HOME"); home != "" {
		return home
	}
	if usr, err := user.Current(); err == nil {
		return usr.HomeDir
	}
	return ""
}

// DefaultDataDir is the default data directory to use for the databases and other persistence requirements.
func DefaultDataDir() string {
	// Try to place the data folder in the user's home dir
	home := homeDir()
	if home != "" {
		switch runtime.GOOS {
		case "darwin":
			panic("darwin not supported")
		case "windows":
			panic("windows not supported")
		default:
			return filepath.Join(home, ".orchestrator")
		}
	}
	// As we cannot guess a stable location, return empty and handle later
	return ""
}

func ZnnWrapToOrchestratorWrap(rpcEvent *definition.WrapTokenRequest) events.WrapRequestZnn {
	ans := events.WrapRequestZnn{
		NetworkClass:  rpcEvent.NetworkClass,
		ChainId:       rpcEvent.ChainId,
		Id:            rpcEvent.Id,
		ToAddress:     rpcEvent.ToAddress,
		TokenAddress:  rpcEvent.TokenAddress,
		Amount:        big.NewInt(0).Set(rpcEvent.Amount),
		Fee:           big.NewInt(0).Set(rpcEvent.Fee),
		Signature:     rpcEvent.Signature,
		RedeemStatus:  UnredeemedStatus,
		SentSignature: false,
	}
	if len(rpcEvent.Signature) > 0 {
		ans.SentSignature = true
	}
	return ans
}

func OrchestratorWrapToZnnWrap(rpcEvent *events.WrapRequestZnn) *definition.WrapTokenRequest {
	ans := &definition.WrapTokenRequest{
		NetworkClass:           rpcEvent.NetworkClass,
		ChainId:                rpcEvent.ChainId,
		Id:                     rpcEvent.Id,
		ToAddress:              rpcEvent.ToAddress,
		TokenAddress:           rpcEvent.TokenAddress,
		Amount:                 big.NewInt(0).Set(rpcEvent.Amount),
		Fee:                    big.NewInt(0).Set(rpcEvent.Fee),
		Signature:              rpcEvent.Signature,
		CreationMomentumHeight: 0,
	}
	return ans
}

func ZnnUnwrapToOrchestatorUnwrap(event *definition.UnwrapTokenRequest) events.UnwrapRequestEvm {
	var status uint32
	if event.Revoked == 1 {
		status = RevokedStatus
	} else if event.Redeemed == 1 {
		status = RedeemedStatus
	} else {
		if len(event.Signature) > 0 {
			status = PendingRedeemStatus
		} else {
			status = UnredeemedStatus
		}
	}
	return events.UnwrapRequestEvm{
		NetworkClass:    event.NetworkClass,
		ChainId:         event.ChainId,
		BlockNumber:     0,
		TransactionHash: common.Hash(event.TransactionHash),
		LogIndex:        event.LogIndex,
		// The Zenon request does not contain the EVM sender or block.
		// Finalized EVM evidence fills these fields before an unsigned row can sign.
		From:         common.Address{},
		To:           event.ToAddress.String(),
		Token:        common.HexToAddress(event.TokenAddress),
		Amount:       event.Amount,
		Signature:    event.Signature,
		RedeemStatus: status,
	}
}

func GetPublicKeyFilePath(dataDir, pubKey string) (string, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return "", err
	}
	pubKeyHex := hex.EncodeToString(pubKeyBytes)
	fileName := fmt.Sprintf("localstate-%s.json", pubKeyHex)
	path := filepath.Join(dataDir, fileName)
	return path, nil
}

func DeletePubKeyFile(dataDir, pubKey string) error {
	filePath, err := GetPublicKeyFilePath(dataDir, pubKey)
	if err != nil {
		return err
	}
	if err := os.Remove(filePath); err != nil {
		return err
	}
	return nil
}

func StateToText(state uint8) string {
	stateStr := ""
	switch state {
	case LiveState:
		stateStr = "LiveState"
	case KeyGenState:
		stateStr = "KeyGenState"
	case HaltedState:
		stateStr = "HaltedState"
	case EmergencyState:
		stateStr = "EmergencyState"
	case ReSignState:
		stateStr = "ReSignState"
	}

	return stateStr
}

// DecompressPubKey turns a base64 compressed secp256k1 key into the base64
// 65-byte uncompressed form (0x04 || X || Y). Each coordinate is padded to 32
// bytes, so the output length never depends on leading zero bytes.
func DecompressPubKey(pubKey string) (string, error) {
	pubKeyBytes, err := base64.StdEncoding.DecodeString(pubKey)
	if err != nil {
		return "", fmt.Errorf("decode compressed pubKey: %w", err)
	}
	if len(pubKeyBytes) != constants.CompressedECDSAPubKeyLength {
		return "", fmt.Errorf("compressed pubKey is %d bytes, want %d", len(pubKeyBytes), constants.CompressedECDSAPubKeyLength)
	}
	key, err := crypto.DecompressPubkey(pubKeyBytes)
	if err != nil {
		return "", fmt.Errorf("decompress pubKey: %w", err)
	}
	dPubKeyBytes := crypto.FromECDSAPub(key)
	if len(dPubKeyBytes) != constants.DecompressedECDSAPubKeyLength {
		return "", fmt.Errorf("decompressed pubKey is %d bytes, want %d", len(dPubKeyBytes), constants.DecompressedECDSAPubKeyLength)
	}
	return base64.StdEncoding.EncodeToString(dPubKeyBytes), nil
}

// ZenonAcceptsDecompressedPubKey reports whether the Zenon bridge contract can
// use this key. The embedded contract (go-zenon ChangeTssECDSAPubKey) builds
// its decompressed key from X.Bytes() and Y.Bytes(), which drop leading zero
// bytes, and then requires 65 bytes when it checks a signature. A key whose X
// or Y starts with a zero byte can therefore never be set or used on Zenon.
// decompressedPubKey is the output of DecompressPubKey.
func ZenonAcceptsDecompressedPubKey(decompressedPubKey string) (bool, error) {
	b, err := base64.StdEncoding.DecodeString(decompressedPubKey)
	if err != nil {
		return false, fmt.Errorf("decode decompressed pubKey: %w", err)
	}
	if len(b) != constants.DecompressedECDSAPubKeyLength {
		return false, fmt.Errorf("decompressed pubKey is %d bytes, want %d", len(b), constants.DecompressedECDSAPubKeyLength)
	}
	return b[1] != 0 && b[33] != 0, nil
}
