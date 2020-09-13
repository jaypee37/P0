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
	ln net.Listener
	connections map[int]connection
}

type connection struct {
	response chan []([]byte)
	conn net.Conn
}

type Request_ struct {
	payload []string
	id int
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
	id := 0
	ln, err := net.Listen("tcp", port_buffer.String())
	if err != nil {
		return err
	}

	input_buffer := make(chan Request_)
	kvs.ln = ln
	go handleCommand(kvs,input_buffer)    

	for {
		// accept and handle client connections
		fmt.Println("listensing")
		conn, err := ln.Accept()
		fmt.Println("Aceepted")
		if err != nil {
			// handle error
			fmt.Println(err)
			return err
		}
		//document connection and handle client
		result_buffer := make(chan []([]byte))
		connection := connection{response:result_buffer,conn:conn}
 		kvs.connections[id] = connection
		go handleConnection(kvs,id,input_buffer)    
		id ++  
	}
	return nil
}

func (kvs *keyValueServer) Close() {
	// TODO: implement this!

	for _,conn := range kvs.connections {
		conn.conn.Close()
	}
	kvs.ln.Close()
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
	if len(data) > 0 && data[len(data)-1] == '\n' {
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

func handleConnection(kvs *keyValueServer,id int, input_buffer chan Request_) {

	// for or select to wait for result buffer to be filled
	fmt.Println("handling connection")
	connection := kvs.connections[id]
	scanner := bufio.NewScanner(connection.conn)
	scanner.Split(SplitColon)

	input := make([]string,0)
	for scanner.Scan() {

		word := scanner.Text()
		input = append(input,word)
	}

	if len(input) < 1 {
		return
	}
	var request  Request_ = Request_{input,id}
	input_buffer <- request
	fmt.Println("Put request on channel",input)

	for {
		select{
		case response := <- connection.response:
			fmt.Println("writing response for request",request,response)

			for _,res := range response {
				connection.conn.Write(res)
			}
			return
			// er := connection.conn.Close()

			// if er != nil {
			// 	fmt.Println(er)
				
			// }
			
			// fmt.Println(er)
			// fmt.Printf("closed connection on port %d\n",port)
		}
	}
	
} 

func handleCommand(kvs *keyValueServer,input_buffer chan Request_){

	for {
		request := <- input_buffer
		fmt.Println("Took request off channel", request.payload)

		command := request.payload
		id := request.id

		connection := kvs.connections[id]
		response_buffer := connection.response
		var data []([]byte)

		// add modify db channel
		switch command[0] {
			case "Put":
				val := []byte(command[2])
				data = HandlePut(kvs,command[1],val)
			case "Get":
				data = HandleGet(kvs,command[1])
			case "Delete":
				data = HandleDelete(kvs,command[1])	
			}
		
		response_buffer <- data
	}
}

func HandlePut(kvs *keyValueServer, key string,value []byte) []([]byte){

	kvs.db.Put(key,value)
	return nil

}

func HandleGet(kvs *keyValueServer,key string) []([]byte){
	
	data := kvs.db.Get(key)
	return data
}

func HandleDelete(kvs *keyValueServer,key string) []([]byte){

	kvs.db.Clear(key)
	return nil
}
