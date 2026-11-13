package main

import (
	"flag"
	"fmt"
	"net"
	// "https://github.com/yangl1996/riblt"
)

func main() {
	fmt.Println("Hello, world!")

	role := flag.String("role", "Sender", "role is either 'Sender' or 'Receiver'")

	flag.Parse()

	fmt.Printf("Hello, %s\n", *role)

	listener, err := net.Listen("tcp", "localhost")
	if err != nil {
		fmt.Println("Unexpected error when upening listener, %e\n", err)
	}

}
