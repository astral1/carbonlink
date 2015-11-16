package client

import (
	"bytes"
	"encoding/binary"

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

func (reply *CarbonlinkReply) LoadBytes(replyBytes []byte) {
	stalecucumber.UnpackInto(reply).From(stalecucumber.Unpickle(bytes.NewReader(replyBytes)))
}
