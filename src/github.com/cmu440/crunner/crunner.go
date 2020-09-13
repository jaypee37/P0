package main

import (
	"fmt"
	"net"
	// "time"
	
)

const (
	defaultHost = "localhost"
	defaultPort = 9999
)

// To test your server implementation, you might find it helpful to implement a
// simple 'client runner' program. The program could be very simple, as long as
// it is able to connect with and send messages to your server and is able to
// read and print out the server's echoed response to standard output. Whether or
// not you add any code to this file will not affect your grade.

func do(com string) {
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		fmt.Println(err)
	}


	// bs :=[]byte(s)
	fmt.Fprintf(conn, com)
}
func main() {
	var s string = "Put:Key:Hello\n"
	var s_ string = "Get:Key\n"
	do(s)
	do(s_)

}
