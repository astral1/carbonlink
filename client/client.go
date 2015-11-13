package client

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"time"

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
	conn    *net.TCPConn
	timeout time.Duration
}

func NewCarbonlink(address *string) *Carbonlink {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)
	conn, err := net.DialTCP("tcp", nil, tcpAddress)
	if err != nil {
		conn = nil
	}

	return &Carbonlink{Address: tcpAddress, conn: conn, timeout: 300 * time.Millisecond}
}

func (cl *Carbonlink) IsValid() bool {
	if cl.conn == nil {
		return false
	}
	zero := make([]byte, 0)
	cl.conn.SetReadDeadline(time.Now())
	if _, err := cl.conn.Read(zero); err == io.EOF {
		cl.conn.Close()
		return false
	}

	return true
}

func (cl *Carbonlink) SetTimeout(timeout time.Duration) {
	cl.timeout = timeout
}

func (cl *Carbonlink) SendRequest(name *string) {
	payload := NewCarbonlinkRequest(name)

	cl.conn.Write(payload.Build())
}

func (cl *Carbonlink) GetReply() (*CarbonlinkReply, bool) {
	var replyLength uint32
	var replyBytes []byte
	bufferdConn := bufio.NewReader(cl.conn)

	err := binary.Read(bufferdConn, binary.BigEndian, &replyLength)

	if err != nil {
		return nil, false
	}

	replyBytes = make([]byte, replyLength)
	binary.Read(bufferdConn, binary.BigEndian, replyBytes)

	reply := NewCarbonlinkReply()
	stalecucumber.UnpackInto(reply).From(stalecucumber.Unpickle(bytes.NewReader(replyBytes)))

	return reply, true
}

func (cl *Carbonlink) Probe(name string, step int) (*CarbonlinkPoints, bool) {
	if cl.conn == nil {
		return nil, false
	}
	cl.conn.SetReadDeadline(time.Now().Add(cl.timeout))
	cl.SendRequest(&name)
	reply, ok := cl.GetReply()

	if !ok {
		return nil, false
	}
	points := NewCarbonlinkPoints(step)
	points.ConvertFrom(reply)
	return points, true
}

func (cl *Carbonlink) Close() {
	if cl.conn != nil {
		cl.conn.Close()
	}
}

func (cl *Carbonlink) Refresh() {
	if cl.conn != nil {
		cl.conn.Close()
	}
	cl.conn, _ = net.DialTCP("tcp", nil, cl.Address)
}
