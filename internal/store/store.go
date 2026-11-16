package store

import (
	"fmt"
	"net"
	"net/http"
	"sync"
	reconpb "use-riblt/internal/store/protobuf"

	"github.com/joonnna/ifrit/digestmaps"
	"google.golang.org/grpc"
)

type itemServer[T comparable] struct {
	mu sync.RWMutex
	reconpb.UnimplementedReconcilerServer
	Store digestmaps.ReverseIndexStore[T, digestSymbol]
	peers []Peer
}

func newItemServer[T comparable]() *itemServer[T] {
	hasher := digestmaps.MustDefaultHasher[T]()
	store, err := digestmaps.NewDigestSet(hasher)
	if err != nil {
		panic("Failed to open reverse index store!\n")
	}

	server := &itemServer[T]{
		Store: store,
		peers: make([]Peer, 0),
	}
	return server
}

func LaunchNewItemServer[T comparable](httpPort, grpcPort int) (<-chan error, error) {
	errCh := make(chan error, 2)

	httpAddr := fmt.Sprintf("0.0.0.0:%d", httpPort)
	grpcAddr := fmt.Sprintf("0.0.0.0:%d", grpcPort)

	/// synchronously bind ports before launching server ////////////////
	httpLis, err := net.Listen("tcp", httpAddr)
	if err != nil {
		return nil, fmt.Errorf("listen HTTP on %d: %w", httpPort, err)
	}
	grpcLis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		_ = httpLis.Close()
		return nil, fmt.Errorf("listen gRPC on %d: %w", grpcPort, err)
	}
	/////////////////////////////////////////////////////////////////////

	server := newItemServer[T]()
	go server.serve(httpLis, grpcLis, errCh)
	return errCh, nil
}

func (is *itemServer[T]) serve(httpLis, grpcLis net.Listener, errCh chan error) error {

	defer is.Store.Close()

	grpcServer := grpc.NewServer()
	defer grpcServer.GracefulStop()

	reconpb.RegisterReconcilerServer(grpcServer, is)

	go func() {
		if err := http.Serve(httpLis, is.ItemRoutes()); err != nil {
			errCh <- err
			grpcServer.Stop()
		}
	}()

	// 1. Open a TCP listener for gRPC
	if err := grpcServer.Serve(grpcLis); err != nil {
		errCh <- err
		return err
	}
	// unless error, grpcServer should have blocked on Serve call
	errCh <- nil
	return nil
}
