// Package signtransport records calls at the go-tss transport boundary.
// Only the isolated signing regression imports it in place of go-tss/tss.
package signtransport

import (
	"encoding/base64"

	"github.com/HyperCore-Team/go-tss/common"
	"github.com/HyperCore-Team/go-tss/keysign"
	"github.com/HyperCore-Team/go-tss/messages"
	gotss "github.com/HyperCore-Team/go-tss/tss"
	"github.com/HyperCore-Team/tss-lib/ecdsa/keygen"
	"github.com/libp2p/go-libp2p-peerstore/addr"
	tcrypto "github.com/tendermint/tendermint/crypto"
)

type TssServer struct {
	*gotss.TssServer
	identity string
}

var Receive func(string, keysign.Request) (keysign.Response, error)

func NewTss(_ addr.AddrList, _ int, identity tcrypto.PrivKey, _, _ string, _ common.TssConfig, _ *keygen.LocalPreParams, _ string, _ messages.Algo, _ map[string]bool) (*TssServer, error) {
	return &TssServer{identity: base64.StdEncoding.EncodeToString(identity.PubKey().Bytes())}, nil
}
func (s *TssServer) KeySign(req keysign.Request) (keysign.Response, error) {
	return Receive(s.identity, req)
}
func (*TssServer) Stop() {}
