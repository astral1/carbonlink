package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"net"
	"sync"

	"github.com/hydrogen18/stalecucumber"
)

type CarbonlinkRequest struct {
	Type   string
	Metric string
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

type CarbonlinkPoints struct {
	Datapoints map[int]float64
	From       int
	Until      int
	Step       int
}

func NewCarbonlinkPoints(step int) *CarbonlinkPoints {
	return &CarbonlinkPoints{Step: step, Datapoints: make(map[int]float64)}
}

func (p *CarbonlinkPoints) ConvertFrom(reply *CarbonlinkReply) {
	for index, point := range reply.Datapoints {
		bucket := (int(point[0].(int64))/p.Step + 1) * p.Step
		value := point[1].(float64)

		p.Datapoints[bucket] = value
		p.Until = bucket
		if index == 0 {
			p.From = bucket
		}
	}
}

type CarbonlinkReply struct {
	Datapoints [][]interface{}
}

func NewCarbonlinkReply() *CarbonlinkReply {
	return &CarbonlinkReply{}
}

type Carbonlink struct {
	Address  *net.TCPAddr
	Conn     *net.TCPConn
	maxUse   int32
	useCount int32
	mutex    *sync.Mutex
}

func NewCarbonlink(address *string, ttl int32) (*Carbonlink, error) {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)
	conn, err := net.DialTCP("tcp", nil, tcpAddress)
	if err != nil {
		return nil, err
	}

	return &Carbonlink{Address: tcpAddress, Conn: conn, maxUse: ttl, useCount: 0, mutex: &sync.Mutex{}}, nil
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

func (cl *Carbonlink) Probe(name string, step int) *CarbonlinkPoints {
	cl.SendRequest(&name)
	reply := cl.GetReply()

	points := NewCarbonlinkPoints(step)
	points.ConvertFrom(reply)
	return points
}

func (cl *Carbonlink) Close() {
	cl.Conn.Close()
}

func (cl *Carbonlink) Refresh() {
	cl.mutex.Lock()
	cl.useCount = cl.useCount + 1
	if cl.useCount == cl.maxUse {
		cl.Conn.Close()
		cl.Conn, _ = net.DialTCP("tcp", nil, cl.Address)
		cl.useCount = 0
	}
	cl.mutex.Unlock()
}
