// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"bufio"
	"fmt"
	"github.com/cmu440/p0partA/kvstore"
	"net"
	"strconv"
	// "io"
	"bytes"
	"strings"
)

type keyValueServer struct {
	// TODO: implement this!
	db           kvstore.KVStore
	ln           net.Listener
	connections  map[int]connection
	dropped_chan chan int
	inc_active_chan  chan int
	dec_active_chan  chan int

	send_active    chan int
	recieve_active chan int

	send_dropped    chan int
	recieve_dropped chan int

	dropped int
	active  int
}

type Response struct {
	key    string
	values []([]byte)
}
type connection struct {
	response   chan Response
	close_conn bool
	finished   chan bool
	conn       net.Conn
}

type Request_ struct {
	payload  []string
	response chan Response
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	connections := make(map[int]connection)
	drop_chan := make(chan int)
	inc_active_chan := make(chan int)
	dec_active_chan := make(chan int)
	send_active := make(chan int)
	recieve_active := make(chan int)
	send_dropped := make(chan int)
	recieve_dropped := make(chan int)
	server := &keyValueServer{  db: store, 
								connections: connections, 
								dropped_chan: drop_chan, 
								inc_active_chan: inc_active_chan,
								dec_active_chan: dec_active_chan,
								send_active: send_active,
								send_dropped: send_dropped,
								recieve_active: recieve_active,
								recieve_dropped: recieve_dropped,

							}
	return server
}

func (kvs *keyValueServer) Start(port int) error {
	go StartListening(kvs, port)
	return nil
}

func StartListening(kvs *keyValueServer, port int) error {
	// parse port string
	var port_buffer bytes.Buffer
	port_str := strconv.Itoa(port)

	port_buffer.WriteString(":")
	port_buffer.WriteString(port_str)

	// start server
	id := 0
	ln, err := net.Listen("tcp", port_buffer.String())
	if err != nil {
		return err
	}

	// buffer to hold each client request
	input_buffer := make(chan Request_)
	kvs.ln = ln
	go handleCommand(kvs, input_buffer)
	go HandleDropped(kvs)
	go HandleActive(kvs)

	for {
		// accept and handle client connections
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			// fmt.Println(err)
			return err
		}
		//document connection and handle client
		result_buffer := make(chan Response, 500)
		// finished_chan := make(chan bool)
		// connection := connection{response:result_buffer,conn:conn,finished:finished_chan,close_conn: false}
		// kvs.connections[id] = connection
		kvs.inc_active_chan <- 1
		go ClientRead(kvs, conn, result_buffer, input_buffer)
		go ClientWrite(kvs, conn, result_buffer, input_buffer)
		id++
	}
	return nil
}
func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	kvs.ln.Close()
}

func (kvs *keyValueServer) CountActive() int {
	kvs.send_active <- 1
	return <-kvs.recieve_active
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	kvs.send_dropped <- 1
	return <-kvs.recieve_dropped
}

func HandleDropped(kvs *keyValueServer) {
	for {
		select {
		case <-kvs.dropped_chan:
			kvs.dropped++
		case <-kvs.send_dropped:
			kvs.recieve_dropped <- kvs.dropped
		}
	}
}

func HandleActive(kvs *keyValueServer) {
	for {
		select {
		case <-kvs.inc_active_chan:
			kvs.active++
		case <-kvs.dec_active_chan:
			kvs.active--
		case <-kvs.send_active:
			kvs.recieve_active <- kvs.active
		}
	}
}

func ClientRead(kvs *keyValueServer, conn net.Conn, response_buffer chan Response, input_buffer chan Request_) {

	reader := bufio.NewReader(conn)
	count := 0
	for {

		b, er := reader.ReadBytes(byte('\n'))
		count++

		if er != nil {
			break
		}

		str := string(b[0 : len(b)-1])
		input := strings.Split(str, ":")

		if len(input) > 0 {

			var request Request_ = Request_{input, response_buffer}
			input_buffer <- request
		}
	}
	var end_request Request_ = Request_{[]string{"End", "End"}, response_buffer}
	input_buffer <- end_request
	return
}

func ClientWrite(kvs *keyValueServer, conn net.Conn, response_buffer chan Response, input_buffer chan Request_) {

	//wait for 'handleCommand' Thread to put db response on connection channel
	for {

		select {
		case response := <-response_buffer:

			if len(response_buffer) >= 500 {
				break
			}
			if response.values == nil {
				kvs.dropped_chan <- 1
				kvs.dec_active_chan <- 1
				return
			}
			if len(response.values) > 0 {
				for _, res := range response.values {

					val := string(res)
					msg := response.key + ":" + val + "\n"

					_, er := conn.Write([]byte(msg))
					if er != nil {

						fmt.Println("error writeing", er)
					}
				}
			}

		}
	}
}

func handleCommand(kvs *keyValueServer, input_buffer chan Request_) {

	for {
		request := <-input_buffer

		command := request.payload
		response_buffer := request.response
		var data []([]byte)

		// add modify db channel
		switch command[0] {
		case "Put":
			val := []byte(command[2])
			// fmt.Println("Put",command[1])
			data = HandlePut(kvs, command[1], val)
		case "Get":
			// fmt.Println("Get",command[1])
			data = HandleGet(kvs, command[1])
		case "Delete":
			// fmt.Println("delete",command[1])
			data = HandleDelete(kvs, command[1])
		case "End":
			data = nil
		}

		response := Response{command[1], data}
		response_buffer <- response
	}
}

func HandlePut(kvs *keyValueServer, key string, value []byte) []([]byte) {

	var v []([]byte) = make([]([]byte), 0)
	kvs.db.Put(key, value)
	return v

}

func HandleGet(kvs *keyValueServer, key string) []([]byte) {
	data := kvs.db.Get(key)
	return data
}

func HandleDelete(kvs *keyValueServer, key string) []([]byte) {
	var v []([]byte) = make([]([]byte), 0)
	kvs.db.Clear(key)
	return v
}
