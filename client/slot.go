package client

import (
	"time"
)

type CarbonlinkSlot struct {
	connection    *CarbonlinkConn
	lastChecked   time.Time
	validDuration time.Duration
	retry         int
	retryStart    time.Time
	key           int
}

func NewCarbonlinkSlot(address string, validDuration time.Duration, key int) *CarbonlinkSlot {
	conn := NewCarbonlinkConn(&address)
	return &CarbonlinkSlot{connection: conn, lastChecked: time.Now(), validDuration: validDuration, key: key}
}

func (slot *CarbonlinkSlot) SetTimeout(timeout time.Duration) {
	slot.connection.SetTimeout(timeout)
}

func (slot *CarbonlinkSlot) Key() int {
	return slot.key
}

func (slot *CarbonlinkSlot) WaitRetry() bool {
	if slot.retry == 0 {
		return false
	}
	// FIXME: make this value configurable
	const duration = 150 * time.Millisecond
	gap := time.Now().Sub(slot.retryStart)

	weightedWait := time.Duration(slot.retry) * duration

	return gap < weightedWait
}

func (slot *CarbonlinkSlot) Retry() {
	if slot.retry == 0 {
		slot.retryStart = time.Now()
	}
	slot.retry++
}

func (slot *CarbonlinkSlot) ResetRetry() {
	slot.retry = 0
}

func (slot *CarbonlinkSlot) RequireValidation() bool {
	now := time.Now()
	gap := now.Sub(slot.lastChecked)

	return gap >= slot.validDuration
}

func (slot *CarbonlinkSlot) Query(name string, step int) (*CarbonlinkPoints, bool) {
	return slot.connection.Probe(name, step)
}

func (slot *CarbonlinkSlot) IsValid() bool {
	return slot.connection.IsValid()
}

func (slot *CarbonlinkSlot) ValidationAndRefresh(force bool) {
	if force || slot.RequireValidation() {
		if force || !slot.IsValid() {
			slot.connection.Refresh()
		}
		now := time.Now()
		slot.lastChecked = now
	}
}

func (slot *CarbonlinkSlot) Close() {
	slot.connection.Close()
}
