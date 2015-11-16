package client

import "time"

type Carbonlink interface {
	Start()
	Close()
	SetTimeout(time.Duration)
	Query(string, int) *CarbonlinkPoints
}
