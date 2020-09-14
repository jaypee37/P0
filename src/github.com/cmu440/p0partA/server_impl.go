// Implementation of a KeyValueServer. Students should write their code in this file.

package p0partA

import (
	"github.com/cmu440/p0partA/kvstore";
	"fmt";
	"net";
	"strconv";
	"bufio";
	// "io"
	"bytes";
	"strings"
)

type keyValueServer struct {
	// TODO: implement this!
	db kvstore.KVStore
	ln net.Listener
	connections map[int]connection
	dropped_chan chan int
	dropped int
	active int
}

type Response struct {
	key string
	values []([]byte)
}
type connection struct {
	response chan Response
	close_conn bool
	finished chan bool
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
	drop_chan := make(chan int)
	server := &keyValueServer{db : store,connections: connections, dropped_chan: drop_chan}
	return server
}

func (kvs *keyValueServer) Start(port int) error {
	go StartListening(kvs,port)
	return nil
}

func StartListening (kvs *keyValueServer, port int) error {
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
	go handleCommand(kvs,input_buffer)  
	go HandleDropped(kvs)  

	for {
		// accept and handle client connections
		conn, err := ln.Accept()
		if err != nil {
			// handle error
			fmt.Println(err)
			return err
		}
		//document connection and handle client
		result_buffer := make(chan Response)
		finished_chan := make(chan bool)
		connection := connection{response:result_buffer,conn:conn,finished:finished_chan,close_conn: false}
		kvs.active ++
 		kvs.connections[id] = connection
		go handleConnection(kvs,id,input_buffer)    
		id ++  
	}
	return nil
}
func (kvs *keyValueServer) Close() {
	// TODO: implement this!
	for _,conn := range kvs.connections {

		// select {
		// case <- conn.finished: 
		er := conn.conn.Close()
		if er != nil{
			fmt.Println(er)
		}
		close(conn.response)
		// }
		
	}
	close(kvs.dropped_chan)
	kvs.ln.Close()
}

func (kvs *keyValueServer) CountActive() int {
	connections := kvs.connections
	count := 0

	for  range connections {
		count ++
	}
	return count
}

func (kvs *keyValueServer) CountDropped() int {
	// TODO: implement this!
	return kvs.dropped
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

func HandleDropped(kvs *keyValueServer) {
	for {
		select {
		case <- kvs.dropped_chan:
			kvs.dropped ++
			kvs.active --
		}
	}
}

func ClientRead(kvs *keyValueServer,id int, input_buffer chan Request_) {

	connection := kvs.connections[id]
	reader := bufio.NewReader(connection.conn)
	count := 0
	for {

		b,er := reader.ReadBytes(byte('\n'))
		count++

		if er != nil {
			break
		}
		
		str := string(b[0:len(b)-1])
		input := strings.Split(str,":")
		// fmt.Println(str)

		if len(input) > 0 {
			
		var request  Request_ = Request_{input,id}
		input_buffer <- request
		}
	}
	fmt.Printf("Read %d requests from client\n",count)

}

func ClientWrite(kvs *keyValueServer,id int, input_buffer chan Request_) {
	
	//wait for 'handleCommand' Thread to put db response on connection channel
	connection := kvs.connections[id]
	for {
		select{
		case response := <- connection.response:
			if response.values == nil {
				// kvs.dropped_chan <- 1
				// connection.finished <- true
				return
			}
			if len(response.values) > 0 {
				for _,res := range response.values {
					
					val := string(res)
					msg := response.key + ":" + val + "\n"

					// fmt.Println(msg,[]byte(msg))
					_,er := connection.conn.Write([]byte(msg))
					if er != nil {

						fmt.Println("error writeing",er)
					}
				}
			} 
			
			
		}
	}
}

func handleConnection(kvs *keyValueServer,id int, input_buffer chan Request_) {

	connection := kvs.connections[id]
	go ClientRead(kvs,id,input_buffer)
	go ClientWrite(kvs,id,input_buffer)
	<-connection.finished
	fmt.Println("done")
	return

} 

func handleCommand(kvs *keyValueServer,input_buffer chan Request_){

	for {
		request := <- input_buffer

		command := request.payload
		id := request.id

		connection := kvs.connections[id]
		response_buffer := connection.response
		var data []([]byte)

		// add modify db channel
		switch command[0] {
			case "Put":
				val := []byte(command[2])
				// fmt.Println("Put",command[1])
				data = HandlePut(kvs,command[1],val)
			case "Get":
				// fmt.Println("Get",command[1])
				data = HandleGet(kvs,command[1])
			case "Delete":
				// fmt.Println("delete",command[1])
				data = HandleDelete(kvs,command[1])	
			case "End":
				fmt.Println(command)
				data = nil
			}
		
		response := Response{command[1],data}
		response_buffer <- response
	}
}

func HandlePut(kvs *keyValueServer, key string,value []byte) []([]byte){

	var v []([]byte) = make([]([]byte),0)
	kvs.db.Put(key,value)
	return v

}

func HandleGet(kvs *keyValueServer,key string) []([]byte){
	data := kvs.db.Get(key)
	return data
}

func HandleDelete(kvs *keyValueServer,key string) []([]byte){
	var v []([]byte) = make([]([]byte),0)
	kvs.db.Clear(key)
	return v
}
