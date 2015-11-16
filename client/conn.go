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

type CarbonlinkReply struct {
	Datapoints [][]interface{}
}

func NewCarbonlinkReply() *CarbonlinkReply {
	return &CarbonlinkReply{}
}

type CarbonlinkConn struct {
	Address *net.TCPAddr
	conn    *net.TCPConn
	timeout time.Duration
}

func NewCarbonlinkConn(address *string) *CarbonlinkConn {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)

	result := &CarbonlinkConn{Address: tcpAddress, timeout: 300 * time.Millisecond}
	result.Refresh()

	return result
}

func (cl *CarbonlinkConn) IsValid() bool {
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

func (cl *CarbonlinkConn) SetTimeout(timeout time.Duration) {
	cl.timeout = timeout
}

func (cl *CarbonlinkConn) SendRequest(name *string) {
	payload := NewCarbonlinkRequest(name)

	cl.conn.Write(payload.Build())
}

func (cl *CarbonlinkConn) GetReply() (*CarbonlinkReply, bool) {
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

func (cl *CarbonlinkConn) Probe(name string, step int) (*CarbonlinkPoints, bool) {
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

func (cl *CarbonlinkConn) Close() {
	if cl.conn != nil {
		cl.conn.Close()
	}
}

func (cl *CarbonlinkConn) Refresh() {
	if cl.conn != nil {
		cl.conn.Close()
	}
	var err error
	cl.conn, err = net.DialTCP("tcp", nil, cl.Address)
	if err != nil {
		cl.conn = nil
		return
	}

	cl.conn.SetNoDelay(true)
}
