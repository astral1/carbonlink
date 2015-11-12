package client

import (
	"testing"
	"time"

	"github.com/oleiade/lane"
)

// Library study test: duration and time
func TestTimestamp(t *testing.T) {
	start := time.Now()
	end := start.Add(300 * time.Millisecond)

	duration := end.Sub(start)
	if duration != 300*time.Millisecond {
		t.Error("duration must be 300 but that is ", duration)
	}
}

// Library study test: duration and time
func TestLaneQueueEmptyBehavior(t *testing.T) {
	q := lane.NewQueue()

	ret := q.Dequeue()

	if ret != nil {
		t.Error("Dequeueing against empty queue is expected to return nil")
	}

	q.Enqueue(1)
	ret = q.Dequeue()

	if ret != 1 {
		t.Error("Dequeued value is expected to 1, but actual value is ", ret)
	}
}

func TestSlotCreation(t *testing.T) {
	slot, err := NewCarbonlinkSlot("127.0.0.1:7002", 15*time.Second)

	if slot == nil && err == nil {
		t.Error("DO NOT try to connect")
	}
}

func TestRequireValidation(t *testing.T) {
	slot, _ := NewCarbonlinkSlot("127.0.0.1:7002", 15*time.Second)

	if slot == nil {
		t.Skipped()
	} else if slot.RequireValidation(false) {
		t.Error("MAYBE failed. if this call is in 15 seconds")
	}
}
