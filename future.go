package eventbus

import (
	"container/list"
	"sync"

	set "github.com/deckarep/golang-set"
	"github.com/uber-go/atomic"
)

type Action func(rst interface{}) interface{}
type ErrorAction func(error) interface{}

type Future interface {
	IsCompleted() bool
	// Return list of EventResponse and EventBus dispatch error
	// vertical processing error is in EventResponse
	GetResult() (*list.List, error)
}

// failed future is immediately completed with error and nil result
type failedFuture struct {
	error
}

func (f *failedFuture) IsCompleted() bool {
	return true
}

func (f *failedFuture) GetResult() (*list.List, error) {
	return nil, f.error
}

func newFuture(eventID uint64, timeout int64) *futureImpl {
	return &futureImpl{
		completed:        atomic.NewBool(false),
		result:           list.New(),
		eventID:          eventID,
		verticalChecking: nil,
		timeout:          timeout,
		cond:             sync.NewCond(&sync.Mutex{}),
	}
}

type futureImpl struct {
	completed *atomic.Bool
	result    *list.List
	err       error

	eventID          uint64
	verticalChecking set.Set
	timeout          int64
	cond             *sync.Cond
}

func (f *futureImpl) IsCompleted() bool {
	return f.completed.Load()
}

func (f *futureImpl) GetResult() (*list.List, error) {
	if !f.completed.Load() {
		f.cond.L.Lock()
		if f.completed.Load() {
			f.cond.L.Unlock()
			return f.result, f.err
		}

		f.cond.Wait()
	}

	return f.result, f.err
}

func (f *futureImpl) setComplete() {
	f.verticalChecking = nil
	f.completed.Store(true)
	f.cond.L.Lock()
	f.cond.Broadcast()
	f.cond.L.Unlock()
}
