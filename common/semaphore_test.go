package common

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestSemaphore(t *testing.T) {
	semaphore := NewSemaphore()

	var counter uint64

	// 50 routine wait on semaphore
	for i := 0; i < 50; i++ {
		go func() {
			if !semaphore.WaitFor(time.Duration(100) * time.Millisecond) {
				t.Error("semaphore not released correctly")
			}
			atomic.AddUint64(&counter, 1)
		}()
	}

	if counter != 0 {
		t.Error("counter accidentally added")
	}

	// signal all
	semaphore.Signal()
	time.Sleep(time.Millisecond)

	// should ++ 50 times
	if counter != 50 {
		t.Errorf("only %d out of %d routine released", counter, 50)
	}

	// should be ready now
	if !semaphore.WaitFor(100) {
		t.Error("semaphore should be ready")
	}

	semaphore.Reset()

	// should not ready after reset
	if semaphore.WaitFor(100) {
		t.Error("semaphore shouldn't ready")
	}

	// wait for timeout
	for i := 0; i < 50; i++ {
		go func() {
			if semaphore.WaitFor(100) {
				t.Error("semaphore accidentally released")
			}
		}()
	}
}
