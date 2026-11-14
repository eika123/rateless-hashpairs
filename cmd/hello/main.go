package main

import (
	"flag"
	"fmt"
	"io"
	"net/http"
	"strconv"

	// "https://github.com/yangl1996/riblt"
	"encoding/json"
	"errors"

	"github.com/joonnna/ifrit/digestmaps"
)

type GreetingPayload struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

func handleGreeting(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	maxBytes := int64(2 << 21) // 4 MB
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)
	dec := json.NewDecoder(r.Body)
	enc := json.NewEncoder(w)

	var payload GreetingPayload
	if err := dec.Decode(&payload); err != nil {
		var maxBytesErr *http.MaxBytesError
		switch {
		case errors.As(err, &maxBytesErr):
			http.Error(w, "Request body too large", http.StatusRequestEntityTooLarge)
		case errors.Is(err, io.EOF):
		default:
			http.Error(w, "Invalid JSON payload", http.StatusBadRequest)
		}
		return
	}

	name := payload.Name
	msg := payload.Message
	enc.Encode(fmt.Sprintf("Hello %s", name))
	enc.Encode("We received your message: ")
	enc.Encode(fmt.Sprintf("%s", msg))

}

func handleHello(w http.ResponseWriter, r *http.Request) {

	defer r.Body.Close()

	maxBytes := int64(2 << 21) // 4 MB
	r.Body = http.MaxBytesReader(w, r.Body, maxBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		// Check if the error was triggered by exceeding maxBytes
		if maxBytesErr, ok := errors.AsType[*http.MaxBytesError](err); ok {
			msg := fmt.Sprintf("Request body exceeds limit of %d bytes", maxBytesErr.Limit)
			http.Error(w, msg, http.StatusRequestEntityTooLarge)
			return
		}

		// Handle other network/read errors
		http.Error(w, "Failed to read request body", http.StatusBadRequest)
		return
	}

	w.Write([]byte("Hello world! ... you wrote:\n"))
	w.Write(body)
	w.Write([]byte("\n"))
}

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

func (s *itemServer[T]) itemRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /", handleHello)
	mux.HandleFunc("POST /greet", handleGreeting)
	mux.HandleFunc("POST /items", s.handleAddItem)
	mux.HandleFunc("GET /items", s.handleListItems)
	mux.HandleFunc("GET /items/{digest}", s.handleGetItem)
	return mux
}

func main() {
	fmt.Println("Hello, world!")

	role := flag.String("role", "Sender", "role is either 'Sender' or 'Receiver'")
	flag.Parse()
	fmt.Printf("Hello, %s\n", *role)

	server := NewItemServer[string]()
	defer server.Store.Close()
	http.ListenAndServe(":8080", server.itemRoutes())
}
