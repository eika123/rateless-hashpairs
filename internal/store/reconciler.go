package store

import (
	"github.com/yangl1996/riblt"
)

func (is *itemServer[T]) makeEncoder() riblt.Encoder[digestSymbol] {
	enc := riblt.Encoder[digestSymbol]{}
	for _, d := range is.Store.DigestsCopy() {
		enc.AddSymbol(d)
	}
	return enc
}

func (is *itemServer[T]) makeDecoder() riblt.Decoder[digestSymbol] {
	dec := riblt.Decoder[digestSymbol]{}
	for _, d := range is.Store.DigestsCopy() {
		dec.AddSymbol(d)
	}

	return dec
}

type Peer struct {
	Host string `json:"host"` // host address
	Port string `json:"port"` // web socket port
}

// reconcile digests with the remote peer
func (p *Peer) reconcileDigests() {

}
