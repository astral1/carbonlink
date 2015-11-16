package client

import "time"

// The interface to define carbonlink client
type Carbonlink interface {
	// Run tasks needed by client start
	Start()
	// Close client
	Close()
	// Set read timeout
	SetTimeout(time.Duration)
	// Query a metric to carbonlink with step
	Query(string, int) *CarbonlinkPoints
}
