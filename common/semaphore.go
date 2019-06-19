package common

import (
	"container/list"
	"sync"
	"time"

	"github.com/uber-go/atomic"
)

type Semaphore struct {
	ready *atomic.Bool
	// because Go's sync.Cond doesn't support wait timeout, we have to make our own
	lock    *sync.Mutex
	waiters *list.List
}

func NewSemaphore() *Semaphore {
	return &Semaphore{
		ready:   atomic.NewBool(false),
		lock:    &sync.Mutex{},
		waiters: list.New(),
	}
}

func (semaphore *Semaphore) WaitFor(duration time.Duration) bool {
	if semaphore.ready.Load() {
		return true
	}

	semaphore.lock.Lock()
	if semaphore.ready.Load() {
		defer semaphore.lock.Unlock()
		return true
	}
	monitor := make(chan struct{}, 1)
	element := semaphore.waiters.PushBack(monitor)
	semaphore.lock.Unlock()
	select {
	case <-monitor:
	case <-time.After(duration):
	}

	// remove the monitor
	semaphore.lock.Lock()
	defer semaphore.lock.Unlock()
	semaphore.waiters.Remove(element)
	return semaphore.ready.Load()
}

func (semaphore *Semaphore) Signal() {
	semaphore.lock.Lock()
	defer semaphore.lock.Unlock()
	semaphore.ready.Store(true)
	for cn := semaphore.waiters.Front(); cn != nil; cn = cn.Next() {
		cn.Value.(chan struct{}) <- struct{}{}
	}
}

func (semaphore *Semaphore) IsReady() bool {
	return semaphore.ready.Load()
}

func (semaphore *Semaphore) Reset() {
	semaphore.ready.Store(false)
}
