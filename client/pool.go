package client

import (
	"sync"
	"time"

	"github.com/oleiade/lane"
)

type CarbonlinkSlot struct {
	connection    *Carbonlink
	lastChecked   time.Time
	validDuration time.Duration
	retry         int
	retryStart    time.Time
	key           int
}

func NewCarbonlinkSlot(address string, validDuration time.Duration, key int) *CarbonlinkSlot {
	conn := NewCarbonlink(&address)
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

type CarbonlinkPool struct {
	slots       []*CarbonlinkSlot
	emptyResult *CarbonlinkPoints
	readyQueue  *lane.Deque
	mutex       *sync.Mutex
	refresh     chan *CarbonlinkSlot
	reconnect   chan *CarbonlinkSlot
	timeout     time.Duration
}

func NewCarbonlinkPool(address string, size int) *CarbonlinkPool {
	const duration = time.Minute
	slots := make([]*CarbonlinkSlot, size)
	empty := NewCarbonlinkPoints(0)
	queue := lane.NewDeque()
	mutex := &sync.Mutex{}
	refresh := make(chan *CarbonlinkSlot, size)
	reconnect := make(chan *CarbonlinkSlot, size)

	for index, _ := range slots {
		slots[index] = NewCarbonlinkSlot(address, duration, index)
		queue.Prepend(index)
	}

	return &CarbonlinkPool{slots: slots, emptyResult: empty, readyQueue: queue, mutex: mutex, refresh: refresh, reconnect: reconnect}
}

func (pool *CarbonlinkPool) Refresh() {
	for {
		slot := <-pool.refresh

		if slot == nil {
			return
		}

		slot.ValidationAndRefresh(false)
		if !slot.IsValid() {
			pool.reconnect <- slot
			continue
		}

		pool.readyQueue.Append(slot.Key())
	}
}

func (pool *CarbonlinkPool) Reconnect() {
	for {
		slot := <-pool.reconnect

		if slot == nil {
			return
		}

		if slot.WaitRetry() {
			pool.reconnect <- slot
			continue
		}

		slot.ValidationAndRefresh(true)
		if !slot.IsValid() {
			slot.Retry()
			pool.reconnect <- slot
			continue
		}

		slot.ResetRetry()
		pool.Return(slot)
	}
}

func (pool *CarbonlinkPool) StartMaintenance() {
	go pool.Refresh()
	go pool.Reconnect()
}

func (pool *CarbonlinkPool) SetTimeout(timeout time.Duration) {
	for _, slot := range pool.slots {
		slot.SetTimeout(timeout)
	}
}

func (pool *CarbonlinkPool) Borrow() *CarbonlinkSlot {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	for {
		if pool.readyQueue.Empty() {
			return nil
		}

		index := pool.readyQueue.Pop()
		slot := pool.slots[index.(int)]

		if slot.RequireValidation() {
			pool.refresh <- slot
			continue
		}

		return slot
	}
}

func (pool *CarbonlinkPool) Return(slot *CarbonlinkSlot) {
	pool.readyQueue.Prepend(slot.Key())
}

func (pool *CarbonlinkPool) Query(name string, step int) *CarbonlinkPoints {
	slot := pool.Borrow()
	if slot == nil {
		return pool.emptyResult
	}

	result, success := slot.Query(name, step)
	if !success {
		pool.reconnect <- slot
		return pool.emptyResult
	}

	pool.Return(slot)
	return result
}

func (pool *CarbonlinkPool) Close() {
	pool.refresh <- nil
	pool.reconnect <- nil
	for _, slot := range pool.slots {
		defer slot.Close()
	}
}
