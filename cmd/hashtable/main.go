package main

import (
	"flag"
	"log"

	"github.com/eika123/rateless-hashpairs/internal/store"
)

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
