package client

import (
	"time"
)

type CarbonlinkSlot struct {
	connection    *Carbonlink
	lastChecked   time.Time
	validDuration time.Duration
}

func NewCarbonlinkSlot(address string, validDuration time.Duration) (*CarbonlinkSlot, error) {
	conn, err := NewCarbonlink(&address)
	if err != nil {
		return nil, err
	}
	return &CarbonlinkSlot{connection: conn, lastChecked: time.Now(), validDuration: validDuration}, nil
}

func (slot *CarbonlinkSlot) RequireValidation() bool {
	now := time.Now()
	gap := now.Sub(slot.lastChecked)

	return gap >= slot.validDuration
}

func (slot *CarbonlinkSlot) Query(name string, step int) (*CarbonlinkPoints, bool) {
	return slot.connection.Probe(name, step)
}

func (slot *CarbonlinkSlot) ValidationAndRefresh() {
	if slot.RequireValidation() {
		if !slot.connection.IsValid() {
			slot.connection.Refresh()
		}
	}
	now := time.Now()
	slot.lastChecked = now
}

type CarbonlinkQueue struct {
	data   []int
	cursor int
	size   int
}

func NewCarbonlinkQueue(size int) *CarbonlinkQueue {
	data := make([]int, size)

	return &CarbonlinkQueue{data: data, cursor: -1, size: size}
}

type CarbonlinkPool struct {
	slots      []*CarbonlinkSlot
	empty      *CarbonlinkPoints
	readyQueue *CarbonlinkQueue
}

func (pool *CarbonlinkPool) NewCarbonlinkPool() *CarbonlinkPool {
	return &CarbonlinkPool{slots: make([]*CarbonlinkSlot, 1), empty: NewCarbonlinkPoints(), readyQueue: NewCarbonlinkQueue(1)}
}

func (pool *CarbonlinkPool) Query(name string, step int) *CarbonlinkPoints {
	return pool.empty
}
