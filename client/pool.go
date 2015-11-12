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
		if !slot.connection.IsValid() {
			slot.connection.Refresh()
		}
	}
	now := time.Now()
	slot.lastChecked = now
}

func (slot *CarbonlinkSlot) Close() {
	slot.connection.Close()
}

type CarbonlinkPool struct {
	slots       []*CarbonlinkSlot
	emptyResult *CarbonlinkPoints
	readyQueue  *lane.Queue
	mutex       *sync.Mutex
}

func NewCarbonlinkPool(address string, size int) *CarbonlinkPool {
	const duration = 10 * time.Second
	slots := make([]*CarbonlinkSlot, size)
	empty := NewCarbonlinkPoints(0)
	queue := lane.NewQueue()
	mutex := &sync.Mutex{}

	if len(address) == 0 {
		return &CarbonlinkPool{slots: slots, emptyResult: empty, readyQueue: queue, mutex: mutex}
	}

	for index, _ := range slots {
		slots[index], _ = NewCarbonlinkSlot(address, duration, index)
		queue.Enqueue(index)
	}

	return &CarbonlinkPool{slots: slots, emptyResult: empty, readyQueue: queue, mutex: mutex}
}

func (pool *CarbonlinkPool) Borrow() *CarbonlinkSlot {
	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	if pool.readyQueue.Empty() {
		return nil
	}

	index := pool.readyQueue.Dequeue()

	return pool.slots[index.(int)]
}

func (pool *CarbonlinkPool) Return(slot *CarbonlinkSlot) {
	pool.readyQueue.Enqueue(slot.Key())
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
	for _, slot := range pool.slots {
		defer slot.Close()
	}
}
