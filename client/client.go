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
		bucket := (int(point[0].(int64)) / p.Step) * p.Step
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
	Address *net.TCPAddr
	Conn    *net.TCPConn
	mutex   *sync.Mutex
}

func NewCarbonlink(address *string) (*Carbonlink, error) {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)
	conn, err := net.DialTCP("tcp", nil, tcpAddress)
	if err != nil {
		return nil, err
	}

	return &Carbonlink{Address: tcpAddress, Conn: conn, mutex: &sync.Mutex{}}, nil
}

func (cl *Carbonlink) IsValid() bool {
	testName := ""
	cl.SendRequest(&testName)
	var replyLength uint32
	bufferdConn := bufio.NewReader(cl.Conn)

	binary.Read(bufferdConn, binary.BigEndian, &replyLength)

	return replyLength != 0
}

func (cl *Carbonlink) SendRequest(name *string) {
	payload := NewCarbonlinkRequest(name)

	cl.Conn.Write(payload.Build())
}

func (cl *Carbonlink) GetReply() (*CarbonlinkReply, bool) {
	var replyLength uint32
	var replyBytes []byte
	bufferdConn := bufio.NewReader(cl.Conn)

	binary.Read(bufferdConn, binary.BigEndian, &replyLength)

	if replyLength == 0 {
		return nil, false
	}

	replyBytes = make([]byte, replyLength)
	binary.Read(bufferdConn, binary.BigEndian, replyBytes)

	reply := NewCarbonlinkReply()
	stalecucumber.UnpackInto(reply).From(stalecucumber.Unpickle(bytes.NewReader(replyBytes)))

	return reply, true
}

func (cl *Carbonlink) Probe(name string, step int) (*CarbonlinkPoints, bool) {
	cl.SendRequest(&name)
	reply, ok := cl.GetReply()

	if !ok {
		return nil, false
	}
	points := NewCarbonlinkPoints(step)
	points.ConvertFrom(reply)
	return points, true
}

func (cl *Carbonlink) RefreshAndRetry(name string, step int) (*CarbonlinkPoints, bool) {
	cl.Refresh()
	return cl.Probe(name, step)
}

func (cl *Carbonlink) Close() {
	cl.Conn.Close()
}

func (cl *Carbonlink) Refresh() {
	cl.mutex.Lock()
	cl.Conn.Close()
	cl.Conn, _ = net.DialTCP("tcp", nil, cl.Address)
	cl.mutex.Unlock()
}
