package tss

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"unsafe"

	"github.com/HyperCore-Team/go-tss/conversion"
	"github.com/HyperCore-Team/go-tss/messages"
	tstorage "github.com/HyperCore-Team/go-tss/storage"
	gotss "github.com/HyperCore-Team/go-tss/tss"
	"github.com/libp2p/go-libp2p-core/peer"
	"github.com/libp2p/go-libp2p-peerstore/addr"
	ma "github.com/multiformats/go-multiaddr"

	"orchestrator/common"
	oconfig "orchestrator/common/config"
)

const addressBookFileName = "address_book.seed"

var errInvalidAddressBook = errors.New("malformed address book")

// secureLocalStateManager implements go-tss's storage.LocalStateManager on top
// of a single verified, open 0700 directory descriptor. Every key share read
// and write is anchored via *at syscalls (no pathname reopen, no symlink
// following) and writes shares directly with mode 0600 via atomic temp+rename
// instead of go-tss's 0655 WriteFile.
type secureLocalStateManager struct {
	dir *oconfig.OwnedDir
}

func newSecureLocalStateManager(dir *oconfig.OwnedDir) *secureLocalStateManager {
	return &secureLocalStateManager{dir: dir}
}

func (s *secureLocalStateManager) keyShareFileName(pubKey string, algo messages.Algo) (string, error) {
	ok, err := conversion.CheckKeyOnCurve(pubKey, algo)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", errors.New("invalid pubkey for file name")
	}
	return common.GetPublicKeyFileName(pubKey)
}

func (s *secureLocalStateManager) SaveLocalState(state tstorage.KeygenLocalState, algo messages.Algo) error {
	buf, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("fail to marshal KeygenLocalState to json: %w", err)
	}
	fileName, err := s.keyShareFileName(state.PubKey, algo)
	if err != nil {
		return err
	}
	return s.dir.WriteFileAtomic(fileName, buf, oconfig.KeyShareFilePerm)
}

func (s *secureLocalStateManager) GetLocalState(pubKey string, algo messages.Algo) (tstorage.KeygenLocalState, error) {
	if len(pubKey) == 0 {
		return tstorage.KeygenLocalState{}, errors.New("pub key is empty")
	}
	fileName, err := s.keyShareFileName(pubKey, algo)
	if err != nil {
		return tstorage.KeygenLocalState{}, err
	}
	buf, found, err := s.dir.ReadFile(fileName, oconfig.KeyShareFilePerm)
	if err != nil {
		return tstorage.KeygenLocalState{}, err
	}
	if !found {
		return tstorage.KeygenLocalState{}, os.ErrNotExist
	}
	var localState tstorage.KeygenLocalState
	if err := json.Unmarshal(buf, &localState); err != nil {
		return tstorage.KeygenLocalState{}, fmt.Errorf("fail to unmarshal KeygenLocalState: %w", err)
	}
	return localState, nil
}

func (s *secureLocalStateManager) SaveAddressBook(address map[peer.ID]addr.AddrList) error {
	var buf bytes.Buffer
	for p, addrs := range address {
		for _, a := range addrs {
			if strings.Contains(a.String(), "127.0.0.1") {
				continue
			}
			record := a.String() + "/p2p/" + p.String() + "\n"
			if _, err := buf.WriteString(record); err != nil {
				return errors.New("fail to write the record to buffer")
			}
		}
	}
	return s.dir.WriteFileAtomic(addressBookFileName, buf.Bytes(), oconfig.ConfigFilePerm)
}

func (s *secureLocalStateManager) RetrieveP2PAddresses() (addr.AddrList, error) {
	input, found, err := s.dir.ReadFile(addressBookFileName, oconfig.ConfigFilePerm)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, os.ErrNotExist
	}
	lines := strings.Split(string(input), "\n")
	var peerAddresses []ma.Multiaddr
	for _, el := range lines {
		if len(el) == 0 {
			continue
		}
		a, err := ma.NewMultiaddr(el)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", errInvalidAddressBook, err)
		}
		peerAddresses = append(peerAddresses, a)
	}
	return peerAddresses, nil
}

// installSecureStateManager replaces go-tss's default FileStateMgr on server
// with mgr before any Keygen/KeySign call runs. It accepts *gotss.TssServer or
// a test transport wrapper embedding *gotss.TssServer (e.g. the bend overlay's
// signtransport.TssServer), and validates the stateManager field name and type
// so any incompatible dependency change fails startup loudly instead of
// silently reverting to insecure 0655 pathname storage.
func installSecureStateManager(server any, mgr tstorage.LocalStateManager) error {
	if server == nil {
		return errors.New("tss server is nil")
	}
	var target *gotss.TssServer
	if s, ok := server.(*gotss.TssServer); ok {
		target = s
	} else {
		v := reflect.ValueOf(server)
		if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
			return errors.New("unexpected tss server representation")
		}
		embedded := v.Elem().FieldByName("TssServer")
		if !embedded.IsValid() || embedded.Type() != reflect.TypeOf((*gotss.TssServer)(nil)) {
			return errors.New("unexpected tss server representation")
		}
		if embedded.IsNil() {
			// Test-only transport stub with no underlying go-tss server.
			return nil
		}
		target = embedded.Interface().(*gotss.TssServer)
	}
	if target == nil {
		return errors.New("tss server is nil")
	}
	v := reflect.ValueOf(target)
	field := v.Elem().FieldByName("stateManager")
	wantType := reflect.TypeOf((*tstorage.LocalStateManager)(nil)).Elem()
	if !field.IsValid() || field.Type() != wantType || !field.CanAddr() {
		return errors.New("tss server does not expose a compatible stateManager field")
	}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Set(reflect.ValueOf(mgr))
	return nil
}
