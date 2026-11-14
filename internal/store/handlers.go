package store

import (
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/joonnna/ifrit/digestmaps"
)

func (s *itemServer[T]) handleAddItem(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	r.Body = http.MaxBytesReader(w, r.Body, 2<<21)
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

func (s *itemServer[T]) handleListItems(w http.ResponseWriter, r *http.Request) {
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

type itemServer[T comparable] struct {
	Store digestmaps.ReverseIndexStore[T, digestmaps.DigestSymbol]
}

func NewItemServer[T comparable]() *itemServer[T] {
	hasher := digestmaps.MustDefaultHasher[T]()
	store, err := digestmaps.NewDigestSet(hasher)
	if err != nil {
		panic("Failed to open reverse index store!\n")
	}

	server := &itemServer[T]{
		Store: store,
	}
	return server
}

func (s *itemServer[T]) ItemRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /items", s.handleAddItem)
	mux.HandleFunc("GET /items", s.handleListItems)
	mux.HandleFunc("GET /items/{digest}", s.handleGetItem)
	return mux
}
