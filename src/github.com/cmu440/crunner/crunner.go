package main

import (
	"fmt"
	"net"
	
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
func main() {
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		fmt.Println(err)
	}

	var s string = "Put:Key:Hello"

	bs :=[]byte(s)
	conn.Write(bs)
	// fmt.Fprintf(conn, "Get:Key\n")
	// _, _ := bufio.NewReader(conn).ReadString('\n')
}
