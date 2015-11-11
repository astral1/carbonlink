package client

import (
	"testing"
	"time"
)

func TestTimestamp(t *testing.T) {
	start := time.Now()
	end := start.Add(300 * time.Millisecond)

	duration := end.Sub(start)
	if duration != 300*time.Millisecond {
		t.Error("duration must be 300 but that is ", duration)
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
	} else if !slot.RequireValidation() {
		t.Error("MAYBE failed. if this call is in 15 seconds")
	}
}
