package client

import (
	"time"
)

type carbonlinkSlot struct {
	connection        *CarbonlinkConn
	lastChecked       time.Time
	validDuration     time.Duration
	retry             int
	retryStart        time.Time
	retryBaseInterval time.Duration
	key               int
}

func newCarbonlinkSlot(address string, validDuration time.Duration, key int) *carbonlinkSlot {
	conn := NewCarbonlinkConn(&address)
	return &carbonlinkSlot{connection: conn, lastChecked: time.Now(), validDuration: validDuration, key: key, retryBaseInterval: 500 * time.Millisecond}
}

func (slot *carbonlinkSlot) SetTimeout(timeout time.Duration) {
	slot.connection.SetTimeout(timeout)
}

func (slot *carbonlinkSlot) Key() int {
	return slot.key
}

func (slot *carbonlinkSlot) SetValidDuration(interval time.Duration) {
	slot.validDuration = interval
}

func (slot *carbonlinkSlot) SetBaseRetryInterval(interval time.Duration) {
	slot.retryBaseInterval = interval
}

func (slot *carbonlinkSlot) WaitRetry() bool {
	if slot.retry == 0 {
		return false
	}
	gap := time.Now().Sub(slot.retryStart)

	weightedWait := time.Duration(slot.retry) * slot.retryBaseInterval

	return gap < weightedWait
}

func (slot *carbonlinkSlot) Retry() {
	if slot.retry == 0 {
		slot.retryStart = time.Now()
	}
	slot.retry++
}

func (slot *carbonlinkSlot) ResetRetry() {
	slot.retry = 0
}

func (slot *carbonlinkSlot) RequireValidation() bool {
	now := time.Now()
	gap := now.Sub(slot.lastChecked)

	return gap >= slot.validDuration
}

func (slot *carbonlinkSlot) Query(name string, step int) (*CarbonlinkPoints, bool) {
	return slot.connection.Probe(name, step)
}

func (slot *carbonlinkSlot) IsValid() bool {
	return slot.connection.IsValid()
}

func (slot *carbonlinkSlot) ValidationAndRefresh(force bool) {
	if force || slot.RequireValidation() {
		if force || !slot.IsValid() {
			slot.connection.Refresh()
		}
		now := time.Now()
		slot.lastChecked = now
	}
}

func (slot *carbonlinkSlot) Close() {
	slot.connection.Close()
}
