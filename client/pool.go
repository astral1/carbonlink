package client

import (
	"sync"
	"time"

	"github.com/oleiade/lane"
)

// Carbonlink Connection Pool
type CarbonlinkPool struct {
	slots       []*carbonlinkSlot
	emptyResult *CarbonlinkPoints
	readyQueue  *lane.Deque
	mutex       *sync.Mutex
	refresh     chan *carbonlinkSlot
	reconnect   chan *carbonlinkSlot
	timeout     time.Duration
}

// Create a new carbonlink connection pool.
func NewCarbonlinkPool(address string, size int) CarbonlinkPool {
	// FIXME: make this value configurable
	const duration = time.Minute
	slots := make([]*carbonlinkSlot, size)
	empty := NewCarbonlinkPoints(0)
	queue := lane.NewDeque()
	mutex := &sync.Mutex{}
	refresh := make(chan *carbonlinkSlot, size)
	reconnect := make(chan *carbonlinkSlot, size)

	for index, _ := range slots {
		slots[index] = newCarbonlinkSlot(address, duration, index)
		queue.Prepend(index)
	}

	return CarbonlinkPool{slots: slots, emptyResult: empty, readyQueue: queue, mutex: mutex, refresh: refresh, reconnect: reconnect}
}

func (pool CarbonlinkPool) runRefresh() {
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

func (pool CarbonlinkPool) runReconnect() {
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
		pool.returnSlot(slot)
	}
}

// Run connection test and refresh goroutine and reconnect handling goroutine
func (pool CarbonlinkPool) Start() {
	go pool.runRefresh()
	go pool.runReconnect()
}

// Set read timeout
func (pool CarbonlinkPool) SetTimeout(timeout time.Duration) {
	for _, slot := range pool.slots {
		slot.SetTimeout(timeout)
	}
}

// Set retry connect interval. default is 300 ms
func (pool CarbonlinkPool) SetBaseRetryInterval(interval time.Duration) {
	for _, slot := range pool.slots {
		slot.SetBaseRetryInterval(interval)
	}
}

func (pool CarbonlinkPool) borrowSlot() *carbonlinkSlot {
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

func (pool CarbonlinkPool) returnSlot(slot *carbonlinkSlot) {
	pool.readyQueue.Prepend(slot.Key())
}

// Query a metric to carbonlink with fixed step.
// DO NOT expand glob and regex
func (pool CarbonlinkPool) Query(name string, step int) *CarbonlinkPoints {
	slot := pool.borrowSlot()
	if slot == nil {
		return pool.emptyResult
	}

	result, success := slot.Query(name, step)
	if !success {
		pool.reconnect <- slot
		return pool.emptyResult
	}

	pool.returnSlot(slot)
	return result
}

// Close carbonlink connection pool.
// And finalize goroutines for maintenance
func (pool CarbonlinkPool) Close() {
	pool.refresh <- nil
	pool.reconnect <- nil
	for _, slot := range pool.slots {
		defer slot.Close()
	}
}
