// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"github.com/cmu440/p0partA/kvstore";
	"fmt";
	"net";
	"strconv";
	"bufio";
	"bytes";
)

type keyValueServer struct {
	// TODO: implement this!
	db kvstore.KVStore
	connections map[int]connection
}

type connection struct {
	buffer chan []byte
	conn net.Conn
}

type Request_ struct {
	payload []string
	port int
}

// New creates and returns (but does not start) a new KeyValueServer.
func New(store kvstore.KVStore) KeyValueServer {
	// TODO: implement this!
	connections := make(map[int]connection)
	server := &keyValueServer{db : store,connections: connections}
	return server
}

func (kvs *keyValueServer) Start(port int) error {

	// parse port string
	var port_buffer bytes.Buffer
	port_str := strconv.Itoa(port)

	port_buffer.WriteString(":")
	port_buffer.WriteString(port_str)

	fmt.Println(port_buffer.String())

	// start server
	ln, err := net.Listen("tcp", port_buffer.String())
	if err != nil {
		return err
	}

	input_buffer := make(chan Request_)
	go handleCommand(kvs,input_buffer)    

	for {
		// accept and handle client connections
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println(err)
			return err
		}
		//document connection and handle client
		result_buffer := make(chan []byte)
		connection := connection{buffer:result_buffer,conn:conn}
 		kvs.connections[port] = connection
		go handleConnection(kvs,port,input_buffer)      
	}
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!

	for _,conn := range kvs.connections {
		conn.conn.Close()
	}
}

func (kvs *keyValueServer) CountActive() int {
	// TODO: implement this!
	return -1
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return -1
}


func dropCR(data []byte) []byte {
	if len(data) > 0 && data[len(data)-1] == '\r' {
		return data[0 : len(data)-1]
	}
	return data
}

// TODO: add additional methods/functions below!
func SplitColon(data []byte, atEOF bool) (advance int, token []byte, err error) {

	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	if i := bytes.IndexByte(data, ':'); i >= 0 {
		// We have a full newline-terminated line.
		return i + 1, dropCR(data[0:i]), nil
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		return len(data), dropCR(data), nil
	}
	// Request more data.
	return 0, nil, nil

}

func handleConnection(kvs *keyValueServer,port int, input_buffer chan Request_) {

	// for or select to wait for result buffer to be filled
	fmt.Println("handling connection")
	connection := kvs.connections[port]
	scanner := bufio.NewScanner(connection.conn)
	scanner.Split(SplitColon)

	input := make([]string,0)
	for scanner.Scan() {

		word := scanner.Text()
		input = append(input,word)
	}

	fmt.Println("here is request",input)
	var request  Request_ = Request_{input,port}
	input_buffer <- request

	for {
		select{
		case <- connection.buffer:
			fmt.Println("write response")
			er := connection.conn.Close()

			if er != nil {
				fmt.Println(er)
				return
			}
			
			fmt.Println(er)
			fmt.Printf("closed connection on port %d\n",port)
			return
		}
	}
	
} 

func handleCommand(kvs *keyValueServer,input_buffer chan Request_){

	for {
		request := <- input_buffer

		command := request.payload
		port := request.port
		fmt.Println("here is command", command)

		connection := kvs.connections[port]
		response_buffer := connection.buffer

		response_buffer <- make([]byte,10)


		//add modify db channel
		// switch word {
		// 	case "Put":
		// 		HandlePut(kvs,input[1],input[2])
		// 	case "Get":
		// 		HandlePut(kvs,input[1],input[2])
		// 	case "Delete":
		// 		HandlePut(kvs,input[1],input[2])	
		// 	}

		}
}

// func HandlePut(key string,value []byte){

// }

// func HandleGet(key string){

// }

// func HandleDelete(key string){

// }
