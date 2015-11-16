package client

import (
	"bufio"
	"encoding/binary"
	"io"
	"net"
	"time"
)

// TCP Connection wrapper for carbonlink
type CarbonlinkConn struct {
	Address *net.TCPAddr
	conn    *net.TCPConn
	timeout time.Duration
}

// Create new connection
func NewCarbonlinkConn(address *string) *CarbonlinkConn {
	tcpAddress, _ := net.ResolveTCPAddr("tcp", *address)

	result := &CarbonlinkConn{Address: tcpAddress, timeout: 300 * time.Millisecond}
	result.Refresh()

	return result
}

// Detect disconnected connection
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

// Set timeout
func (cl *CarbonlinkConn) SetTimeout(timeout time.Duration) {
	cl.timeout = timeout
}

func (cl *CarbonlinkConn) sendRequest(name *string) {
	payload := NewCarbonlinkRequest(name)

	cl.conn.Write(payload.Build())
}

func (cl *CarbonlinkConn) getReply() (*CarbonlinkReply, bool) {
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
	reply.LoadBytes(replyBytes)

	return reply, true
}

// Query metric data with step from carbonlink
func (cl *CarbonlinkConn) Probe(name string, step int) (*CarbonlinkPoints, bool) {
	if cl.conn == nil {
		return nil, false
	}
	cl.conn.SetReadDeadline(time.Now().Add(cl.timeout))
	cl.sendRequest(&name)
	reply, ok := cl.getReply()

	if !ok {
		return nil, false
	}
	points := NewCarbonlinkPoints(step)
	points.ConvertFrom(reply)
	return points, true
}

// Close connection
func (cl *CarbonlinkConn) Close() {
	if cl.conn != nil {
		cl.conn.Close()
	}
}

// Reconnect insanity connection
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
