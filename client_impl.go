// Contains the implementation of a LSP client.

package lsp

import (
	"encoding/json"
	"errors"
	"github.com/cmu440/lspnet"
	"time"
)

type cliente struct {
	connID int

	connection *lspnet.UDPConn

	serveraddr *lspnet.UDPAddr

	inmess chan *Message
	//in messages - sequencenum to payload
	inmessages   map[int]*Message
	nextprocess  int
	returntoread chan *Message

	//outmess chan *Message
	//out messages - sequence number to payload
	outmessages map[int]*mBackoff
	outseqnum   int

	takefromqueue chan bool
	addtoqueue    chan []*Message

	GQsizereq   chan bool
	GQsize      int
	GQsizeget   chan int
	globalQueue []*Message

	outqueue [][]byte
	OQTake   chan bool
	OQAdd    chan []byte
	OQRet    chan []byte

	writeTriggerTest chan bool

	recievedConnAck chan bool

	startClose    chan bool
	finishClose   chan bool
	isClosing     bool
	readFromClose chan bool
	EGQClose      chan bool
	EOQClose      chan bool

	OQSizeReq chan bool
	OQSizeRet chan int

	GQTake      chan bool
	GQTakeChan  chan *Message
	GQIsClosing bool

	nextProcessRead int
}

// NewClient creates, initiates, and returns a new client. This function
// should return after a connection with the server has been established
// (i.e., the client has received an Ack message from the server in response
// to its connection request), and should return a non-nil error if a
// connection could not be made (i.e., if after K epochs, the client still
// hasn't received an Ack message from the server in response to its K
// connection requests).
//
// hostport is a colon-separated string identifying the server's host address
// and port number (i.e., "localhost:9999").
func NewClient(hostport string, params *Params) (Client, error) {
	//fmt.Print("starting client\n")
	addr, err := lspnet.ResolveUDPAddr("udp", hostport)
	if err != nil {
		return nil, err
	}
	conn, err := lspnet.DialUDP("udp", nil, addr)

	if err != nil {
		return nil, err
	}
	cli := &cliente{
		connID:           0,
		connection:       conn,
		serveraddr:       addr,
		inmess:           make(chan *Message),
		inmessages:       map[int]*Message{},
		nextprocess:      1,
		returntoread:     make(chan *Message),
		outmessages:      make(map[int]*mBackoff),
		outseqnum:        1,
		takefromqueue:    make(chan bool),
		addtoqueue:       make(chan []*Message),
		GQsizereq:        make(chan bool),
		GQsize:           0,
		GQsizeget:        make(chan int),
		globalQueue:      []*Message{},
		outqueue:         [][]byte{},
		OQTake:           make(chan bool),
		OQAdd:            make(chan []byte),
		OQRet:            make(chan []byte),
		recievedConnAck:  make(chan bool),
		writeTriggerTest: make(chan bool),
		startClose:       make(chan bool),
		finishClose:      make(chan bool),
		isClosing:        false,
		readFromClose:    make(chan bool),
		EGQClose:         make(chan bool),
		EOQClose:         make(chan bool),
		OQSizeReq:        make(chan bool),
		OQSizeRet:        make(chan int),
		GQTake:           make(chan bool),
		GQTakeChan:       make(chan *Message),
		GQIsClosing:      false,
		nextProcessRead:  1,
	}
	initConn := NewConnect()
	connBO := &mBackoff{
		m:           initConn,
		age:         0,
		currbackoff: 0,
	}
	cli.outmessages[0] = connBO

	//write out the message, the clientRoutine will receive and process the ack
	//The message will also be resent every epoch

	writeToServer(conn, initConn)
	go cli.readFrom()
	go cli.editglobalqueue()
	go cli.clientRoutine(params)
	go cli.editOutQueue()

	//processed once we recieve connection acknowledgement from server

	<-cli.recievedConnAck
	return cli, nil
}

func writeToServer(conn *lspnet.UDPConn, mess *Message) {
	ret, _ := json.Marshal(mess)
	conn.Write(ret)
}

func (cli *cliente) editglobalqueue() {
	for {
		select {

		case <-cli.EGQClose:
			cli.GQIsClosing = true
			return

		case globapp := <-cli.addtoqueue:
			cli.globalQueue = append(cli.globalQueue, globapp...)
		case <-cli.GQsizereq:
			//log.Println("CLIENT: GLOBAL SIZE REQ")
			cli.GQsizeget <- len(cli.globalQueue)

		case <-cli.GQTake:
			//log.Println("CLIENT: GQTake")
			if len(cli.globalQueue) == 0 {
				cli.GQTakeChan <- nil
			} else {
				cli.GQTakeChan <- cli.globalQueue[0]
				cli.globalQueue = cli.globalQueue[1:]
			}
		}
	}
}

//helps with checksum calculation, used by server and client
func checksumHelper(mess *Message) uint16 {
	sum := Int2Checksum(mess.ConnID)
	sum += Int2Checksum(mess.SeqNum)
	sum += Int2Checksum(mess.Size)
	sum += ByteArray2Checksum(mess.Payload)
	return uint16(sum)
}

//Routine for outQueue manipulation
func (cli *cliente) editOutQueue() {
	for {
		select {
		case <-cli.EOQClose:
			return
		case msg := <-cli.OQAdd:
			cli.outqueue = append(cli.outqueue, msg)
		case <-cli.OQTake:
			if len(cli.outqueue) == 0 {
				cli.OQRet <- nil
			} else {
				ret := cli.outqueue[0]
				cli.outqueue = cli.outqueue[1:]
				cli.OQRet <- ret
			}
		case <-cli.OQSizeReq:
			cli.OQSizeRet <- len(cli.outqueue)
		}
	}
}

//looking at all incoming messages
func (cli *cliente) readFrom() {
	for {
		select {
		case <-cli.readFromClose:
			return
		default:
			payl := make([]byte, 200)
			m := &Message{}
			numbytes, _, err := cli.connection.ReadFromUDP(payl)
			//Compare numbytes with length of payload
			//Convert payload to checksum
			//Compare with checksum
			payl = payl[0:numbytes]
			err = json.Unmarshal(payl, m)
			if m.Type == MsgData {
				if len(m.Payload) < m.Size {
					continue
				}
				if len(m.Payload) > m.Size {
					m.Payload = m.Payload[0:m.Size]
				}
			}
			if err == nil {
				//Sending message to client routines
				cli.inmess <- m
			}
		}
	}
}

func increaseBackoff(currB int, maxB int) int {
	if currB <= 0 {
		return 1
	} else {
		newB := 2 * currB
		if newB > maxB {
			return maxB
		}
		return newB
	}
}

func (cli *cliente) checkCanSend(params *Params) bool {
	lowestUnackedOut := cli.outseqnum
	sentData := false
	//setting lowestUnackedOut
	for seqnum, _ := range cli.outmessages {
		if lowestUnackedOut > seqnum {
			lowestUnackedOut = seqnum
		}
	}
	upperbnd := lowestUnackedOut + params.WindowSize - 1 //inclusive upper bound for sending
	for {
		//fmt.Println("CLIENT: CAN WE SEND")
		if cli.outseqnum <= upperbnd && len(cli.outmessages) < params.MaxUnackedMessages {
			cli.OQTake <- true
			pld := <-cli.OQRet
			if pld != nil {
				newmess := NewData(cli.connID, cli.outseqnum, len(pld), pld, 0)
				newmess.Checksum = checksumHelper(newmess)
				newBO := &mBackoff{
					m:           newmess,
					age:         0,
					currbackoff: 0,
				}
				cli.outmessages[newmess.SeqNum] = newBO
				cli.outseqnum++
				sentData = true
				writeToServer(cli.connection, newmess)
			} else {
				break
			}
		} else {
			break
		}
	}
	return sentData
}

func (cli *cliente) clientRoutine(params *Params) {
	currepoch := 0 //tracks how many epochs it's been since a message recieved from server
	sentData := false
	for {
		if cli.isClosing {
			if len(cli.outmessages) == 0 {
				//Close the rest of the routines:readFrom, editOutQueue, cli.OQ()
				if len(cli.outqueue) == 0 {
					//send signals to global queue and outqueue routines
					cli.connection.Close()
					cli.EGQClose <- true
					cli.EOQClose <- true
					cli.readFromClose <- true
					time.Sleep(time.Millisecond * 100)
					cli.finishClose <- true
					//Set a close boolean, check for those in other routines
					return
				}
			}
		}
		select {
		case <-cli.startClose:
			//log.Print("CLIENT: Received close request")
			cli.isClosing = true
		case <-time.After(time.Duration(params.EpochMillis) * time.Millisecond):
			//increase epoch count
			currepoch++
			//if the connect message is still unacked, send it again
			_, connExists := cli.outmessages[0] // cli.outmessages[0] will always be the connect message
			if connExists {
				writeToServer(cli.connection, NewConnect())
				continue
			}

			resentData := false
			//incrementing the ages of all outmessages and sending again if needed
			for _, mb := range cli.outmessages {
				mb.age += 1
				if mb.age > mb.currbackoff {
					mb.currbackoff = increaseBackoff(mb.currbackoff, params.MaxBackOffInterval)
					mb.age = 0
					resentData = true
					writeToServer(cli.connection, mb.m)
				}
			}
			if currepoch >= params.EpochLimit {
				//fmt.Println("CLIENT: Epoch limit reached")
				cli.addtoqueue <- []*Message{NewAck(cli.connID, -1)}
				//cli.isClosing = true
			}

			//if we haven't resent data or sent data, we send heartbeat: resentdata and sentData need to be false
			if !resentData && !sentData {
				heartbeat := NewAck(cli.connID, 0)
				writeToServer(cli.connection, heartbeat)
			}
			sentData = false

		case mess := <-cli.inmess:
			//reset epochs if we get any message
			currepoch = 0

			switch mess.Type {
			case MsgData:
				//check integrity and checksum
				//Checksum checking
				if checksumHelper(mess) != mess.Checksum {
					continue
				}
				//Already been recieved
				if mess.SeqNum < cli.nextprocess {
					ack := NewAck(cli.connID, mess.SeqNum)
					writeToServer(cli.connection, ack)
					continue
				}

				_, inDict := cli.inmessages[mess.SeqNum]

				if inDict {
					ack := NewAck(cli.connID, mess.SeqNum)
					writeToServer(cli.connection, ack)
					continue
				}
				//processing the message
				if cli.nextprocess == mess.SeqNum {
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
					cli.addtoqueue <- globapp
				} else {
					//put the message into the inqueue in order
					cli.inmessages[mess.SeqNum] = mess
				}
				ack := NewAck(cli.connID, mess.SeqNum)
				writeToServer(cli.connection, ack)
				//send back ack

			case MsgAck:
				outmsg, inDict := cli.outmessages[mess.SeqNum]
				if inDict {
					if outmsg.m.Type == MsgConnect {
						//fmt.Print("Connect ACK received!")
						cli.connID = mess.ConnID
						cli.recievedConnAck <- true
					}
					delete(cli.outmessages, mess.SeqNum)
					//fmt.Println("CLIENT: Deleted:"+ strconv.Itoa(mess.SeqNum))
					didSend := cli.checkCanSend(params)
					if didSend {
						sentData = true
					}
				}
			}
		//triggered on every write to check if we can send a new data message
		case <-cli.writeTriggerTest:
			didSend := cli.checkCanSend(params)
			if didSend {
				sentData = true
			}
		}
	}
}

func (c *cliente) ConnID() int {
	return c.connID
}

func (c *cliente) Read() ([]byte, error) {
	for {

		c.GQTake <- true
		select {
		case ret := <-c.GQTakeChan:
			if ret == nil {
				time.Sleep(time.Millisecond * 1)
				continue
			}
			if ret.Type == MsgAck {
				return nil, errors.New("Client: READ ERROR")
			}
			return ret.Payload, nil
		}
	}

}

func (c *cliente) Write(payload []byte) error {
	//create the message
	c.OQAdd <- payload
	c.writeTriggerTest <- true
	return nil
	//send the mesasge into the channel
}

func (c *cliente) Close() error {
	c.startClose <- true
	<-c.finishClose
	return nil
}
