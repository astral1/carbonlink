package carbonlink

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"

	"github.com/hydrogen18/stalecucumber"
)

type CarbonlinkRequest struct {
	Type   string
	Metric string
	Key    string
	Value  string
}

func NewEmptyCarbonlinkRequest() *CarbonlinkRequest {
	return &CarbonlinkRequest{}
}

func NewCarbonlinkRequest(metricName *string) *CarbonlinkRequest {
	return &CarbonlinkRequest{Type: "cache-query", Metric: *metricName}
}

func (req *CarbonlinkRequest) Build() []byte {
	requestBuf := new(bytes.Buffer)
	payloadBuf := new(bytes.Buffer)

	stalecucumber.NewPickler(payloadBuf).Pickle(req)

	binary.Write(requestBuf, binary.BigEndian, uint32(payloadBuf.Len()))
	binary.Write(requestBuf, binary.BigEndian, payloadBuf.Bytes())

	return requestBuf.Bytes()
}

type CarbonlinkReply struct {
	Datapoints []interface{}
}

func NewCarbonlinkReply() *CarbonlinkReply {
	return &CarbonlinkReply{}
}

type Carbonlink struct {
	Address *net.TCPAddr
	Conn    *net.TCPConn
}

func NewCarbonlink(address *string) (*Carbonlink, error) {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)
	conn, err := net.DialTCP("tcp", nil, tcpAddress)
	if err == nil {
		defer conn.Close()
	} else {
		return nil, err
	}

	return &Carbonlink{Address: tcpAddress, Conn: conn}, nil
}

func (cl *Carbonlink) SendRequest(name *string) {
	payload := NewCarbonlinkRequest(name)

	cl.Conn.Write(payload.Build())
}

func (cl *Carbonlink) GetReply() *CarbonlinkReply {
	var replyLength uint32
	var replyBytes []byte
	bufferdConn := bufio.NewReader(cl.Conn)

	binary.Read(bufferdConn, binary.BigEndian, &replyLength)

	replyBytes = make([]byte, replyLength)
	binary.Read(bufferdConn, binary.BigEndian, replyBytes)

	reply := NewCarbonlinkReply()
	stalecucumber.UnpackInto(reply).From(stalecucumber.Unpickle(bytes.NewReader(replyBytes)))

	return reply
}

func (cl *Carbonlink) Close() {
	cl.Conn.Close()
}
