package store

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"sync"
	reconpb "github.com/eika123/rateless-hashpairs/internal/store/protobuf"

	"github.com/joonnna/ifrit/digestmaps"
	"github.com/yangl1996/riblt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type ReconcileSummary struct {
	Peer             string `json:"peer"`
	DigestsRequested int    `json:"digests_requested"` // A \ B sent to peer
	DigestsAvailable int    `json:"digests_available"` // B \ A fetched from peer
	ItemsSent        int    `json:"items_sent"`
	ItemsReceived    int    `json:"items_received"`
}

type digestSymbol = digestmaps.DigestSymbol

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
	Port string `json:"port"` // rpc inter-peer socket port
}

type reconcileResult struct {
	resp *reconpb.ReconcileResponse
	err  error
}

// client-side RPC
func (is *itemServer[T]) reconcileDigestsWithPeer(ctx context.Context, p Peer) (*reconpb.ReconcileResponse, error) {
	addr := net.JoinHostPort(p.Host, p.Port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Since ReconcileDigests is a streaming RPC, this call just opens the stream.
	stream, err := reconpb.NewReconcilerClient(conn).ReconcileDigests(ctx)
	if err != nil {
		return nil, err
	}

	decodedCh := make(chan struct{})
	doneCh := make(chan reconcileResult, 1)
	go receiveReconcileResponse(stream, decodedCh, doneCh)

	enc := is.makeEncoder()
	return sendCodedSymbols(stream, &enc, decodedCh, doneCh)
}

func receiveReconcileResponse(
	stream grpc.BidiStreamingClient[reconpb.ReconcileRequest, reconpb.ReconcileResponse],
	decodedCh chan struct{},
	doneCh chan<- reconcileResult,
) {
	var decodedOnce sync.Once
	agg := &reconpb.ReconcileResponse{}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			doneCh <- reconcileResult{resp: agg}
			return
		}
		if err != nil {
			doneCh <- reconcileResult{err: err}
			return
		}

		if resp.Decoded {
			agg.Decoded = true
			decodedOnce.Do(func() { close(decodedCh) })
		}
		agg.RequestedDigests = append(agg.RequestedDigests, resp.RequestedDigests...)
		agg.AvailableDigests = append(agg.AvailableDigests, resp.AvailableDigests...)
	}
}

func sendCodedSymbols(
	stream grpc.BidiStreamingClient[reconpb.ReconcileRequest, reconpb.ReconcileResponse],
	enc *riblt.Encoder[digestSymbol],
	decodedCh <-chan struct{},
	doneCh <-chan reconcileResult,
) (*reconpb.ReconcileResponse, error) {
	const batchSize = 16

	for {
		select {
		case <-decodedCh:
			// Remote peer decoded; stop sending and wait for Recv() to finish draining.
			_ = stream.CloseSend()
			res := <-doneCh
			return res.resp, res.err
		case res := <-doneCh:
			_ = stream.CloseSend()
			return res.resp, res.err
		default:
		}

		batch := make([]*reconpb.CodedSymbol, 0, batchSize)
		for range batchSize {
			cs := enc.ProduceNextCodedSymbol()
			batch = append(batch, &reconpb.CodedSymbol{
				HashedSymbol: encodeHashedSymbol(cs.HashedSymbol),
				Count:        cs.Count,
			})
		}

		if err := stream.Send(&reconpb.ReconcileRequest{Symbols: batch}); err != nil {
			// If server closes the stream while client is sending, Send returns io.EOF;
			// read the real response or error from the Recv() goroutine.
			if err == io.EOF {
				res := <-doneCh
				return res.resp, res.err
			}
			return nil, err
		}
	}
}

// reconcileWithPeer runs Phase 1 (ReconcileDigests) and, if diffs exist, Phase 2 (Sync).
func (is *itemServer[T]) reconcileWithPeer(ctx context.Context, p Peer) (ReconcileSummary, error) {
	summary := ReconcileSummary{Peer: net.JoinHostPort(p.Host, p.Port)}

	resp, err := is.reconcileDigestsWithPeer(ctx, p)
	if err != nil {
		return summary, fmt.Errorf("reconcile digests with %s: %w", summary.Peer, err)
	}

	summary.DigestsRequested = len(resp.RequestedDigests)
	summary.DigestsAvailable = len(resp.AvailableDigests)
	if summary.DigestsRequested == 0 && summary.DigestsAvailable == 0 {
		return summary, nil
	}

	sent, received, err := is.syncWithPeer(ctx, p, resp.RequestedDigests, resp.AvailableDigests)
	summary.ItemsSent = sent
	summary.ItemsReceived = received
	if err != nil {
		return summary, fmt.Errorf("sync with %s: %w", summary.Peer, err)
	}
	return summary, nil
}

// Server side RPC
func (is *itemServer[T]) ReconcileDigests(
	stream grpc.BidiStreamingServer[reconpb.ReconcileRequest, reconpb.ReconcileResponse],
) error {
	dec := is.makeDecoder()

	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}

		for _, s := range req.Symbols {
			//hs, err := DecodeHashedSymbol(s.GetHashedSymbol(), DecodeDigestSymbol)
			hs, err := decodeHashedSymbol(s.GetHashedSymbol())
			if err != nil {
				return err
			}

			dec.AddCodedSymbol(riblt.CodedSymbol[digestSymbol]{
				HashedSymbol: hs,
				Count:        s.Count,
			})
		}
		dec.TryDecode()
		if dec.Decoded() {
			return streamDigestPages(stream, dec)
		}
	}
}

func streamDigestPages(
	stream grpc.BidiStreamingServer[reconpb.ReconcileRequest, reconpb.ReconcileResponse],
	decoder riblt.Decoder[digestSymbol],
) error {
	send := func(req, avail []*reconpb.HashedSymbol) error {
		err := stream.Send(&reconpb.ReconcileResponse{
			Decoded:          true,
			RequestedDigests: req,
			AvailableDigests: avail,
		})
		return err
	}
	const digestPageSize = 4096
	remote := decoder.Remote()
	local := decoder.Local()

	// Fast path: empty diff or fits in a single page.
	if len(remote)+len(local) <= digestPageSize {
		return send(encodeHashedSymbols(remote), encodeHashedSymbols(local))
	}

	for i := 0; i < len(remote); i += digestPageSize {
		end := min(i+digestPageSize, len(remote))
		symbols := remote[i:end]
		if err := send(encodeHashedSymbols(symbols), nil); err != nil {
			return err
		}
	}

	for i := 0; i < len(local); i += digestPageSize {
		end := min(i+digestPageSize, len(local))
		symbols := local[i:end]
		if err := send(nil, encodeHashedSymbols(symbols)); err != nil {
			return err
		}
	}
	return nil
}

type syncStream interface {
	Send(*reconpb.SyncFrame) error
	Recv() (*reconpb.SyncFrame, error)
}

func (is *itemServer[T]) syncWithPeer(
	ctx context.Context,
	p Peer,
	requested, available []*reconpb.HashedSymbol,
) (sent, received int, err error) {
	addr := net.JoinHostPort(p.Host, p.Port)
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return 0, 0, err
	}
	defer conn.Close()

	stream, err := reconpb.NewReconcilerClient(conn).Sync(ctx)
	if err != nil {
		return 0, 0, err
	}

	type recvRes struct {
		count int
		err   error
	}
	recvCh := make(chan recvRes, 1)
	go func() {
		n, err := is.recvSyncFrames(stream, nil)
		recvCh <- recvRes{count: n, err: err}
	}()

	const wantBatchSize = 32768
	for i := 0; i < len(available); i += wantBatchSize {
		end := min(i+wantBatchSize, len(available))
		want := make([][]byte, end-i)
		for j, hs := range available[i:end] {
			want[j] = hs.GetSymbol()
		}
		if err := stream.Send(&reconpb.SyncFrame{
			Payload: &reconpb.SyncFrame_WantDigests{
				WantDigests: &reconpb.WantDigests{Digests: want},
			},
		}); err != nil {
			return 0, 0, err
		}
	}

	digests, err := hashedSymbolsToDigests(requested)
	if err != nil {
		return 0, 0, err
	}
	sent, err = is.sendSyncItems(stream, digests)
	if err != nil {
		return sent, 0, err
	}
	if err := stream.Send(&reconpb.SyncFrame{
		Payload: &reconpb.SyncFrame_Complete{
			Complete: &reconpb.SyncComplete{Fingerprint: digestmaps.EncodeDigestSymbol(is.Store.Fingerprint())},
		},
	}); err != nil {
		return sent, 0, err
	}
	_ = stream.CloseSend()

	res := <-recvCh
	return sent, res.count, res.err
}

// Sync implements the Phase 2 bidirectional item-transfer RPC.
func (is *itemServer[T]) Sync(
	stream grpc.BidiStreamingServer[reconpb.SyncFrame, reconpb.SyncFrame],
) error {
	var sendMu sync.Mutex
	var sendWG sync.WaitGroup
	sendErrCh := make(chan error, 1)

	onWant := func(digests []digestSymbol) {
		sendWG.Add(1)
		go func() {
			defer sendWG.Done()
			sendMu.Lock()
			defer sendMu.Unlock()
			if _, err := is.sendSyncItems(stream, digests); err != nil {
				select {
				case sendErrCh <- err:
				default:
				}
			}
		}()
	}

	if _, err := is.recvSyncFrames(stream, onWant); err != nil {
		return err
	}
	sendWG.Wait()

	select {
	case err := <-sendErrCh:
		return err
	default:
	}

	return stream.Send(&reconpb.SyncFrame{
		Payload: &reconpb.SyncFrame_Complete{
			Complete: &reconpb.SyncComplete{Fingerprint: digestmaps.EncodeDigestSymbol(is.Store.Fingerprint())},
		},
	})
}

func (is *itemServer[T]) sendSyncItems(stream syncStream, digests []digestSymbol) (int, error) {
	const syncChunkBytes = 128 * 1024
	sent := 0

	for _, d := range digests {
		item, found := is.Store.GetByDigest(d)
		if !found {
			continue
		}
		raw, err := json.Marshal(item)
		if err != nil {
			return sent, err
		}
		rawDigest := digestmaps.EncodeDigestSymbol(d)

		if err := stream.Send(&reconpb.SyncFrame{
			Payload: &reconpb.SyncFrame_DataHeader{
				DataHeader: &reconpb.DataItemHeader{
					Digest:    rawDigest,
					TotalSize: int64(len(raw)),
				},
			},
		}); err != nil {
			return sent, err
		}

		for off := 0; off < len(raw); off += syncChunkBytes {
			end := min(off+syncChunkBytes, len(raw))
			if err := stream.Send(&reconpb.SyncFrame{
				Payload: &reconpb.SyncFrame_DataChunk{
					DataChunk: &reconpb.DataItemChunk{Data: raw[off:end]},
				},
			}); err != nil {
				return sent, err
			}
		}

		if err := stream.Send(&reconpb.SyncFrame{
			Payload: &reconpb.SyncFrame_DataEnd{
				DataEnd: &reconpb.DataItemEnd{Digest: rawDigest},
			},
		}); err != nil {
			return sent, err
		}
		sent++
	}
	return sent, nil
}

func (is *itemServer[T]) recvSyncFrames(stream syncStream, onWant func([]digestSymbol)) (int, error) {
	var buf bytes.Buffer
	received := 0

	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return received, nil
		}
		if err != nil {
			return received, err
		}

		switch p := frame.Payload.(type) {
		case *reconpb.SyncFrame_WantDigests:
			if onWant != nil {
				digests := make([]digestSymbol, len(p.WantDigests.Digests))
				for i, raw := range p.WantDigests.Digests {
					d, err := digestmaps.DecodeDigestSymbol(raw)
					if err != nil {
						return received, err
					}
					digests[i] = d
				}
				onWant(digests)
			}
		case *reconpb.SyncFrame_DataHeader:
			buf.Reset()
		case *reconpb.SyncFrame_DataChunk:
			buf.Write(p.DataChunk.Data)
		case *reconpb.SyncFrame_DataEnd:
			var item T
			if err := json.Unmarshal(buf.Bytes(), &item); err != nil {
				return received, fmt.Errorf("decode synced item: %w", err)
			}
			is.Store.Insert(item)
			received++
			buf.Reset()
		case *reconpb.SyncFrame_Complete:
		}
	}
}

func buildHashedSymProto(b []byte, h uint64) *reconpb.HashedSymbol {
	return &reconpb.HashedSymbol{Symbol: b, Hash: h}
}

func encodeHashedSymbol(sym riblt.HashedSymbol[digestSymbol]) *reconpb.HashedSymbol {
	return digestmaps.EncodeHashedSymbol(sym, digestmaps.EncodeDigestSymbol, buildHashedSymProto)
}

func decodeHashedSymbol(sym *reconpb.HashedSymbol) (riblt.HashedSymbol[digestSymbol], error) {
	return digestmaps.DecodeHashedSymbol(sym, digestmaps.DecodeDigestSymbol)
}

func encodeHashedSymbols(syms []riblt.HashedSymbol[digestSymbol]) []*reconpb.HashedSymbol {
	return digestmaps.EncodeHashedSymbols(syms, digestmaps.EncodeDigestSymbol, buildHashedSymProto)
}

func hashedSymbolsToDigests(syms []*reconpb.HashedSymbol) ([]digestSymbol, error) {
	return digestmaps.DecodeSymbols(syms, digestmaps.DecodeDigestSymbol)
}
