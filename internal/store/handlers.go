package store

import (
	"encoding/json"
	"fmt"
	"log"
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

func (s *itemServer[T]) handleAddItemsBatch(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, 2<<21) // 4MB
	var items []T
	if err := json.NewDecoder(r.Body).Decode(&items); err != nil {
		http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		return
	}
	added := s.Store.AddElements(items...)
	w.Header().Set("Content-Type", "application/json")
	if added > 0 {
		w.WriteHeader(http.StatusCreated)
	}
	json.NewEncoder(w).Encode(map[string]any{
		"added": added,
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
	s.mu.Lock()
	defer s.mu.Unlock()
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
	s.mu.RLock()
	defer s.mu.RUnlock()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(s.peers)
}

func (s *itemServer[T]) handleReconcile(w http.ResponseWriter, r *http.Request) {
	s.mu.RLock()
	peers := make([]Peer, len(s.peers))
	copy(peers, s.peers)
	s.mu.RUnlock()

	if len(peers) == 0 {
		http.Error(w, "no peers registered", http.StatusBadRequest)
		return
	}

	summaries := make([]ReconcileSummary, 0, len(peers))
	for _, p := range peers {
		summary, err := s.reconcileWithPeer(r.Context(), p)
		if err != nil {
			log.Printf("reconcile error: %v", err)
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		summaries = append(summaries, summary)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(summaries)
}

func (s *itemServer[T]) ItemRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /items", s.handleAddItem)
	mux.HandleFunc("POST /items/batch", s.handleAddItemsBatch)
	mux.HandleFunc("GET /items", s.handleListItems)
	mux.HandleFunc("GET /items/{digest}", s.handleGetItem)
	mux.HandleFunc("POST /peers", s.handleRegisterPeer)
	mux.HandleFunc("GET /peers", s.handleListPeers)
	mux.HandleFunc("POST /reconcile", s.handleReconcile)
	return mux
}
