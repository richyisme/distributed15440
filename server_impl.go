// Contains the implementation of a LSP server.

package lsp

import (
	"encoding/json"
	"errors"
	//"log"
	"github.com/cmu440/lspnet"
	"strconv"
	"time"
)

type client struct {
	TMPTRACKER int

	address      *lspnet.UDPAddr //so the server knows how to write back
	connectionID int

	//map of (unacked) messages outgoing, mapping seqnum to message
	outmessages map[int]*mBackoff
	// number for the least number that hasn't been acked
	lowestNotAcked int

	inmess chan *Message
	//queue of messages incoming - mapping seqnum to message
	inmessages map[int]*Message
	//since messages must be processed 1,2,3,4 by seq number, increment nextprocess once that message has been
	//processed/read by API,
	//starts at 1, once inmessages[nextprocess] exists, then it removes it, puts it on global queue,
	//increments nextprocess
	nextprocess int

	//////////////////////////////

	//outmess   chan *Message //when the server sends message, put onto this channel
	outseqnum int //when the server sends messages to client, puts on outqueue, assigns outseqnumber

	outQueue [][]byte
	OQAdd    chan []byte
	OQTake   chan bool
	OQRet    chan []byte

	writeTriggerTest chan bool

	startClosing chan bool
	isClosing    bool
	doneClosing  chan bool

	OQSizeReq chan bool
	OQSizeRet chan int

	OQClose chan bool
}

type mBackoff struct {
	m           *Message
	age         int
	currbackoff int
}

type messaddr struct {
	m       *Message
	address *lspnet.UDPAddr
}
type cliAndID struct {
	cli *client
	id  int
}

type server struct {
	// TODO: Implement this!
	address    *lspnet.UDPAddr
	connection *lspnet.UDPConn
	//clients map from conn id to client pointer
	clients     map[int]*client
	globalqueue []*Message
	GQsize      int

	addtoqueue chan []*Message

	messagein chan messaddr
	//glob messages, slice that will be read by Read API

	returntoread chan *Message

	takefromqueue chan bool
	getGQsize     chan bool
	GQsizechan    chan int

	checkInClients    chan int
	clientFromClients chan *client

	startClose    chan bool
	startedClose  bool
	finishedClose chan bool

	GQClose    chan bool
	GQTake     chan bool
	GQTakeChan chan *Message
}

// NewServer creates, initiates, and returns a new server. This function should
// NOT block. Instead, it should spawn one or more goroutines (to handle things
// like accepting incoming client connections, triggering epoch events at
// fixed intervals, synchronizing events using a for-select loop like you saw in
// project 0, etc.) and immediately return. It should return a non-nil error if
// there was an error resolving or listening on the specified port number.
func NewServer(port int, params *Params) (Server, error) {
	addr, err := lspnet.ResolveUDPAddr("udp", "localhost:"+strconv.Itoa(port))
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.ListenUDP("udp", addr)
	if err != nil {
		return nil, err
	}
	s := &server{
		address:           addr,
		connection:        conn,
		clients:           make(map[int]*client),
		globalqueue:       []*Message{},
		GQsize:            0,
		addtoqueue:        make(chan []*Message),
		messagein:         make(chan messaddr),
		returntoread:      make(chan *Message, 1),
		takefromqueue:     make(chan bool),
		getGQsize:         make(chan bool),
		GQsizechan:        make(chan int),
		checkInClients:    make(chan int),
		clientFromClients: make(chan *client),
		startClose:        make(chan bool),
		startedClose:      false,
		finishedClose:     make(chan bool),
		GQClose:           make(chan bool),
		GQTake:            make(chan bool),
		GQTakeChan:        make(chan *Message),
	}
	//creating the server object
	go s.server_routine(params)

	go s.server_receive()

	go s.editGlobalQueue()

	return s, nil
}

func (s *server) editGlobalQueue() {
	for {
		//fmt.Print("EDITQUEUE")
		select {
		case <-s.GQClose:
			return
		case globapp := <-s.addtoqueue:
			s.globalqueue = append(s.globalqueue, globapp...)
			s.GQsize += len(globapp)
		case <-s.getGQsize:
			s.GQsizechan <- len(s.globalqueue)
		case <-s.GQTake:
			if len(s.globalqueue) == 0 {
				s.GQTakeChan <- nil
			} else {
				s.GQTakeChan <- s.globalqueue[0]
				s.globalqueue = s.globalqueue[1:]
			}

		}
	}
}

func (s *server) server_receive() {
	for {
		payl := make([]byte, 20000)
		var m Message

		numbytes, addr, err := s.connection.ReadFromUDP(payl)
		payl = payl[0:numbytes]
		json.Unmarshal(payl, &m)

		//size checking for data messages
		if m.Type == MsgData {
			if len(m.Payload) < m.Size {
				continue
			}
			if len(m.Payload) > m.Size {
				m.Payload = m.Payload[0:m.Size]
			}
		}
		if err == nil {

			ma := messaddr{}
			ma.m = &m
			ma.address = addr
			s.messagein <- ma
		}
	}
}

func (s *server) server_routine(params *Params) {
	serveConnID := 1
	for {
		//ALL THE CLIENTS ARE CLOSED AT THIS POINT
		if s.startedClose {
			s.getGQsize <- true
			GQL := <-s.GQsizechan
			//Has to wait for the GQ to have no more messages to READ
			if GQL == 0 {
				//Kill the GQ routine, server receive routine
				s.addtoqueue <- []*Message{NewAck(-1, -1)}
				time.Sleep(10 * time.Millisecond)
				s.GQClose <- true
				s.finishedClose <- true
				return
			}

		}
		select {
		case <-s.startClose:
			s.startedClose = true
		case checkNum := <-s.checkInClients:
			c, inDict := s.clients[checkNum]
			if inDict {
				s.clientFromClients <- c
			} else {
				s.clientFromClients <- nil
			}

		case ma := <-s.messagein:

			mess := ma.m
			addr := ma.address
			switch mess.Type {
			case MsgConnect:
				//check to make sure the new client isn't already in client map
				var existingclient *client = nil

				cliexists := false
				for key, element := range s.clients {
					if element.address == addr {
						cliexists = true
						existingclient = s.clients[key]
						break
					}
				}

				if !cliexists {
					//create a new client
					cli := &client{
						TMPTRACKER:     1,
						address:        addr,
						connectionID:   serveConnID,
						outmessages:    make(map[int]*mBackoff),
						lowestNotAcked: 1,
						inmess:         make(chan *Message),
						inmessages:     make(map[int]*Message),
						nextprocess:    1,
						//outmess:         make(chan *Message),
						outseqnum:        1,
						outQueue:         [][]byte{},
						OQAdd:            make(chan []byte),
						OQTake:           make(chan bool),
						OQRet:            make(chan []byte),
						writeTriggerTest: make(chan bool, 1),
						startClosing:     make(chan bool),
						isClosing:        false,
						doneClosing:      make(chan bool),
						OQSizeReq:        make(chan bool),
						OQSizeRet:        make(chan int),
						OQClose:          make(chan bool),
					}

					//put the client into the map, with the key being connection_id, and the valu being the client (not *)
					s.clients[serveConnID] = cli
					//start the write routine for the client
					go cli.clientWriteRoutine(s, params)
					go cli.editClientOutQueue()
					//send the ack for the connect to the client
					newack := NewAck(serveConnID, 0)
					writeToClient(s, newack, addr)

					//increment serveConnID
					serveConnID++
				} else { //client exists already, stored in existingclient
					writeToClient(s, NewAck(existingclient.connectionID, 0), addr)

				}

			default:
				_, cliexists := s.clients[mess.ConnID]
				if cliexists {
					cli := s.clients[mess.ConnID]
					cli.inmess <- mess
				}
			}

		}

	}
}

func writeToClient(s *server, mess *Message, addr *lspnet.UDPAddr) {
	ret, _ := json.Marshal(mess)
	s.connection.WriteToUDP(ret, addr)
}

func (s *server) Read() (int, []byte, error) {
	for {

		s.GQTake <- true
		//log.Println("CLIENT: Take sent")
		select {
		case ret := <-s.GQTakeChan:
			if ret == nil {
				time.Sleep(time.Millisecond * 1)
				continue
			}
			if ret.Type == MsgAck {
				return 0, nil, errors.New("SERVER: READ ERROR")
			}
			//log.Println("SERVER: READING: " + ret.String())
			return ret.ConnID, ret.Payload, nil
		}
	}
}

func (s *server) Write(connId int, payload []byte) error {

	s.checkInClients <- connId
	cli := <-s.clientFromClients
	cli.writeTriggerTest <- true
	if cli != nil {

		//WRITING FROM SERVER TO CLIENT

		cli.OQAdd <- payload
		return nil
	} else {
		return errors.New("bad write")
	}
	//puts the message it onto the outmessages queue for that client in server's client map
}

func (cli *client) editClientOutQueue() {
	for {
		select {
		case <-cli.OQClose:
			cli.doneClosing <- true
			return
		case pld := <-cli.OQAdd:
			cli.outQueue = append(cli.outQueue, pld)
		case <-cli.OQTake:
			if len(cli.outQueue) == 0 {
				cli.OQRet <- nil
			} else {
				ret := cli.outQueue[0]
				cli.outQueue = cli.outQueue[1:]
				cli.OQRet <- ret
			}
		case <-cli.OQSizeReq:
			cli.OQSizeRet <- len(cli.outQueue)
		}
	}
}

func (s *server) CloseConn(connId int) error {
	//Just have to close the client
	//log.Println("SERVER:CLOSE CONN CALLED")
	s.checkInClients <- connId
	cli := <-s.clientFromClients
	if cli != nil {

		cli.startClosing <- true
		<-cli.doneClosing
		//log.Println("SERVER: Received CLOSE CONN Command")
		return nil
	} else {
		return errors.New("SERVER: CLOSECONN: Client doesn't exist")
	}
}

func (s *server) Close() error {
	//Loop through all the clients, run close conn
	s.startClose <- true
	<-s.finishedClose
	return nil
	return errors.New("not yet implemented")
}

func getLowestUnacked(osn int, cli *client) int {
	lowestUnackedOut := osn
	for seqnum, _ := range cli.outmessages {
		if lowestUnackedOut > seqnum {
			lowestUnackedOut = seqnum
		}
	}
	return lowestUnackedOut
}

func (s *server) sCheckCanSend(cli *client, params *Params) bool {
	lowestUnackedOut := getLowestUnacked(cli.outseqnum, cli)
	snt := false
	for seqnum, _ := range cli.outmessages {
		if lowestUnackedOut > seqnum {
			lowestUnackedOut = seqnum
		}
	}

	upperbnd := lowestUnackedOut + params.WindowSize - 1 //inclusive upper bound for sending
	for {
		if cli.outseqnum <= upperbnd && len(cli.outmessages) < params.MaxUnackedMessages {
			cli.OQTake <- true
			pld := <-cli.OQRet
			if pld != nil {
				newmess := NewData(cli.connectionID, cli.outseqnum, len(pld), pld, 0)
				newmess.Checksum = checksumHelper(newmess)
				mback := &mBackoff{
					m:           newmess,
					age:         0,
					currbackoff: 0,
				}
				cli.outmessages[cli.outseqnum] = mback
				cli.outseqnum++
				writeToClient(s, newmess, cli.address)
				snt = true
			} else {
				break
			}
		} else {
			break
		}
	}
	return snt
}

func (cli *client) clientWriteRoutine(s *server, params *Params) {
	//puts messages on the outmessages and
	currepoch := 0 //how long since we've recieved ANY message
	//currentBackoff := 0 //current backoff
	//lastKnownReceivedACK := 0 //how long since we've received ACK
	sentData := false //did we send a data message, reset every epoch
	for {
		if cli.isClosing {
			cli.OQSizeReq <- true
			OQL := <-cli.OQSizeRet
			if len(cli.outmessages) == 0 && OQL == 0 {

				//Got to kill: outqueuea
				cli.OQClose <- true
				//SERVER:Client Routine Closed

				return
			}
		}
		select {
		case <-cli.startClosing:
			cli.isClosing = true
		case <-time.After(time.Duration(params.EpochMillis) * time.Millisecond):
			currepoch++

			//increment all backoffs in the outmessages, check and send the messages again

			if currepoch >= params.EpochLimit {
				//Epoch limit reached
				s.addtoqueue <- []*Message{NewAck(cli.connectionID, -1)}

			}

			resent := false
			for _, messb := range cli.outmessages {
				messb.age = messb.age + 1
				if messb.age > messb.currbackoff {
					messb.age = 0
					messb.currbackoff = increaseBackoff(messb.currbackoff, params.MaxBackOffInterval)
					writeToClient(s, messb.m, cli.address)
					resent = true
				}
			}

			//if no data was sent or resent, do a heartbeat
			if !resent && !sentData {
				heartbeat := NewAck(cli.connectionID, 0)
				writeToClient(s, heartbeat, cli.address)
			}

			//heartbeats

		case mess := <-cli.inmess:
			currepoch = 0 //reset currepoch for any in message
			switch mess.Type {
			case MsgData:

				//check message integrity and size truncation
				//Checksum checking
				if checksumHelper(mess) != mess.Checksum {
					continue
				}
				//Makes sure the sequence number is greater than or equal to nextprocess, otherwise we've already recieved
				if mess.SeqNum < cli.nextprocess {
					ack := NewAck(mess.ConnID, mess.SeqNum)
					writeToClient(s, ack, cli.address)
					continue
				}
				//What to do if message is already on queue? writeback ack?
				_, inDict := cli.inmessages[mess.SeqNum]

				if inDict {
					ack := NewAck(mess.ConnID, mess.SeqNum)
					writeToClient(s, ack, cli.address)
					continue
				}

				if mess.SeqNum == cli.nextprocess {
					//Move the message into the global queue
					globapp := []*Message{mess}
					cli.nextprocess++
					for {
						_, exists := cli.inmessages[cli.nextprocess]
						if exists {
							globapp = append(globapp, cli.inmessages[cli.nextprocess])
							delete(cli.inmessages, cli.nextprocess)
							cli.nextprocess++
						} else {
							break
						}
					}
					//GLOBAL QUEUE EDIT ADD ALL BYTES FROM globapp
					s.addtoqueue <- globapp
				} else {
					//put the message onto the inqueue in order
					cli.inmessages[mess.SeqNum] = mess
				}
				ack := NewAck(cli.connectionID, mess.SeqNum)
				writeToClient(s, ack, cli.address)

			case MsgAck:

				//delete the message off the outqueue

				_, inDict := cli.outmessages[mess.SeqNum]
				if inDict {
					delete(cli.outmessages, mess.SeqNum)
				}
				didSend := s.sCheckCanSend(cli, params)
				if didSend {
					sentData = true
				}
			}
			//called on writes, and ack receptions
		case <-cli.writeTriggerTest:
			//default:

			didSend := s.sCheckCanSend(cli, params)
			if didSend {
				sentData = true
			}

		}
	}

}
