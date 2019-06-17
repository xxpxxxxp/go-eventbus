package eventbus

import (
	"testing"
	"time"
)

func TestFutureImpl(t *testing.T) {
	future := newFuture(0, 0)

	start := time.Now().UnixNano()
	go func() {
		time.Sleep(time.Duration(100) * time.Millisecond)
		future.result.PushBack("testing")
		future.err = nil
		future.setComplete()
	}()

	if future.IsCompleted() {
		t.Error("future should not be completed at the beginning")
	}

	if rst, err := future.GetResult(); rst == nil || rst.Front().Value != "testing" || err != nil {
		t.Error("future result is not correct")
	}

	if time.Now().UnixNano()-start < 100*1000000 {
		t.Error("future should not be completed now")
	}

	if !future.IsCompleted() {
		t.Error("future should be completed")
	}
}
