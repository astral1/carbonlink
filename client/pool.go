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
	key           int
}

func NewCarbonlinkSlot(address string, validDuration time.Duration, key int) (*CarbonlinkSlot, error) {
	conn, err := NewCarbonlink(&address)
	if err != nil {
		return nil, err
	}
	return &CarbonlinkSlot{connection: conn, lastChecked: time.Now(), validDuration: validDuration, key: key}, nil
}

func (slot *CarbonlinkSlot) SetTimeout(timeout time.Duration) {
	slot.connection.SetTimeout(timeout)
}

func (slot *CarbonlinkSlot) Key() int {
	return slot.key
}

func (slot *CarbonlinkSlot) RequireValidation() bool {
	now := time.Now()
	gap := now.Sub(slot.lastChecked)

	return gap >= slot.validDuration
}

func (slot *CarbonlinkSlot) Query(name string, step int) (*CarbonlinkPoints, bool) {
	return slot.connection.Probe(name, step)
}

func (slot *CarbonlinkSlot) ValidationAndRefresh(force bool) {
	if force || slot.RequireValidation() {
		if force || !slot.connection.IsValid() {
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
	timeout     time.Duration
}

func NewCarbonlinkPool(address string, size int) *CarbonlinkPool {
	const duration = 10 * time.Second
	slots := make([]*CarbonlinkSlot, size)
	empty := NewCarbonlinkPoints(0)
	queue := lane.NewDeque()
	mutex := &sync.Mutex{}
	refresh := make(chan *CarbonlinkSlot, size)

	for index, _ := range slots {
		slots[index], _ = NewCarbonlinkSlot(address, duration, index)
		queue.Prepend(index)
	}

	return &CarbonlinkPool{slots: slots, emptyResult: empty, readyQueue: queue, mutex: mutex, refresh: refresh}
}

func (pool *CarbonlinkPool) Refresh() {
	for {
		slot := <-pool.refresh

		if slot == nil {
			return
		}

		slot.ValidationAndRefresh(false)
		pool.readyQueue.Append(slot.Key())
	}
}

func (pool *CarbonlinkPool) SetTimeout(timeout time.Duration) {
	for _, slot := range pool.slots {
		slot.SetTimeout(timeout)
	}
}

func (pool *CarbonlinkPool) Borrow() *CarbonlinkSlot {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if pool.readyQueue.Empty() {
		return nil
	}

	index := pool.readyQueue.Pop()
	slot := pool.slots[index.(int)]

	if slot.RequireValidation() {
		pool.refresh <- slot
		return nil
	}

	return slot
}

func (pool *CarbonlinkPool) Return(slot *CarbonlinkSlot) {
	pool.readyQueue.Prepend(slot.Key())
}

func (pool *CarbonlinkPool) Query(name string, step int) *CarbonlinkPoints {
	slot := pool.Borrow()
	if slot == nil {
		return pool.emptyResult
	}

	defer pool.Return(slot)

	result, success := slot.Query(name, step)
	if !success {
		slot.ValidationAndRefresh(true)
		return pool.emptyResult
	}

	return result
}

func (pool *CarbonlinkPool) Close() {
	pool.refresh <- nil
	for _, slot := range pool.slots {
		defer slot.Close()
	}
}
