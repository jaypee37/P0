package main

import (
	"fmt"
	"net"
	"time"
	"bufio"
	
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

func do(c chan bool) {
	conn, err := net.Dial("tcp", "localhost:9999")
	if err != nil {
		fmt.Println(err)
	}


	// bs :=[]byte(s)
	// b := make([]byte,10)
	
	for i := 0; i < 50; i++ {
		fmt.Println(i)



		var s string = fmt.Sprintf("Put:Key_%d:val_%d\n",i,i)
		_,err := conn.Write([]byte(s))

		if err != nil {
			fmt.Println(err)
		}
	}
	for i := 0; i < 50; i++ {
		fmt.Println(i)
		var s string = fmt.Sprintf("Get:Key_%d\n",i)
		_,err := conn.Write([]byte(s))

		if err != nil {
			fmt.Println(err)
		}
	}

	time.Sleep(10000 * time.Millisecond)

	scanner := bufio.NewScanner(conn)
	scanner.Split(bufio.ScanLines)
	for {
		if ok := scanner.Scan(); !ok {
			fmt.Println("broke")
            break
		}
		fmt.Println(scanner.Text())
	c <- true
	
	return
	}
}
func main() {
	
	c := make(chan bool)
	go do(c)
	go do(c)
	<-c
	<-c
	return

}
