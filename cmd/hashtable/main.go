package main

import (
	"flag"
	"fmt"
	"io"
	"log"
	"net/http"
	"github.com/eika123/rateless-hashpairs/internal/store"

	// "https://github.com/yangl1996/riblt"
	"encoding/json"
	"errors"
	// "flag"
)

type GreetingPayload struct {
	Name    string `json:"name"`
	Message string `json:"message"`
}

var maxBytes = int64(2 << 21) // 4 MB

func handleGreeting(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
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

func helloRoutes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /greet", handleGreeting)
	mux.HandleFunc("GET /hello", handleHello)
	return mux
}

func main() {
	httpPort := flag.Int("httpPort", 8080, "./hashtable -httpPort=8080")
	grpcPort := flag.Int("grpcPort", 8081, "./hashtable -grpcPort=8081")
	flag.Parse()

	errCh, err := store.LaunchNewItemServer[string](*httpPort, *grpcPort)
	if err != nil {
		log.Fatal(err)
	}
	// waint on itemServer to stop
	if err := <-errCh; err != nil {
		log.Fatal(err)
	}
}
