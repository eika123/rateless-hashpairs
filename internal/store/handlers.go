package store

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"github.com/joonnna/ifrit/digestmaps"
)

func (s *itemServer[T]) handleAddItem(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	maxBytes := int64(2 << 21) // 4MB
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	var item T
	if err := json.NewDecoder(r.Body).Decode(&item); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	digest, added := s.Store.Insert(item)
	w.Header().Set("Content-Type", "application/json")
	if added {
		w.WriteHeader(http.StatusCreated)
	}
	json.NewEncoder(w).Encode(map[string]any{
		"digest": digest,
		"added":  added,
	})
}

func (s *itemServer[T]) handleListItems(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.Store.Elements())
}

func (s *itemServer[T]) handleGetItem(w http.ResponseWriter, r *http.Request) {
	raw := r.PathValue("digest")
	parsed, err := strconv.ParseUint(raw, 10, 64)
	if err != nil {
		http.Error(w, "Invalid digest", http.StatusBadRequest)
		return
	}
	item, found := s.Store.GetByDigest(digestmaps.DigestSymbol(parsed))
	if !found {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(item)
}

func (s *itemServer[T]) handleRegisterPeer(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	var peer Peer
	if err := json.NewDecoder(r.Body).Decode(&peer); err != nil {
		errMsg := fmt.Sprintf(
			"%s. %s\n",
			"Error: Invalid JSON payload for Peer",
			"Expect Peer on form {\"host\":\"<hostname>\", \"port\": \"<portnumber>\"}\n",
		)
		http.Error(w, errMsg, http.StatusBadRequest)
		return
	}

	// check that the received peer is not already known
	for _, p := range s.peers {
		if peer.Host == p.Host && peer.Port == p.Port {
			w.WriteHeader(200)
			return
		}
	}

	s.peers = append(s.peers, peer)
	w.WriteHeader(201)
}

func (s *itemServer[T]) handleListPeers(w http.ResponseWriter, _ *http.Request) {
	json.NewEncoder(w).Encode(s.peers)
}

type digestSymbol = digestmaps.DigestSymbol

type itemServer[T comparable] struct {
	Store digestmaps.ReverseIndexStore[T, digestSymbol]
	peers []Peer
}

func NewItemServer[T comparable]() *itemServer[T] {
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

func (s *itemServer[T]) ItemRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /items", s.handleAddItem)
	mux.HandleFunc("GET /items", s.handleListItems)
	mux.HandleFunc("GET /items/{digest}", s.handleGetItem)
	mux.HandleFunc("POST /peers", s.handleRegisterPeer)
	mux.HandleFunc("GET /peers", s.handleListPeers)
	return mux
}
