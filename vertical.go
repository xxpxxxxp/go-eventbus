package eventbus

import (
	"context"
	"reflect"
	"runtime/debug"
	"sync"

	"github.com/scryner/lfreequeue"
)

type vertical struct {
	AsyncVerticalInterface

	logger Logger

	interests       []reflect.Type
	ctx             context.Context
	cancelFunc      context.CancelFunc
	exitGroup       *sync.WaitGroup
	eventQueue      *lfreequeue.Queue
	eventNotifyChan chan struct{}

	handle   VerticalHandle
	eventBus EventBusWithRespond
}

func (v *vertical) loop() {
	v.exitGroup.Add(1)
	defer v.exitGroup.Done()

	v.OnAttached(v.eventBus)
	defer v.OnDetached(v.eventBus)

	defer func() {
		if r := recover(); r != nil {
			// catch other's shit
			// unexpected panic will stop the world
			v.logger.Errorf("panic in vertical %s, Stack:\n%s", v.Name(), string(debug.Stack()))
			v.eventBus.Shutdown()
		}
	}()

	for {
		if ev, ok := v.eventQueue.Dequeue(); ok {
			select {
			case <-v.ctx.Done():
				return // EventBus is shutting down
			default:
				v.Process(v.eventBus, ev.(Event)) // process the event we got
			}
		} else {
			// no event available now
			select {
			case <-v.ctx.Done():
				return // EventBus is shutting down
			case <-v.eventNotifyChan: // new event add to queue, continue
			}
		}
	}
}

/* What's the difference between async & sync vertical
   They both have interests
   For sync vertical, return value is expected as the resp on the event, thus the processing is synced
   For async vertical, you can respond an event anytime after you got the event. Feel free to use another goroutine: \
     the main processing should not be blocked.
     But, be aware that you can only respond 1 time for 1 event -- do not attempt to respond multiple times
*/

type abstractVerticalInterface interface {
	// although `Name` & `Interests` are function, they should consider as final values
	// DO NOT change the return value after attached
	Name() string
	Interests() []reflect.Type
	OnAttached(bus EventBus)
	OnDetached(bus EventBus)
}

type AsyncVerticalInterface interface {
	abstractVerticalInterface
	// you are expect to call bus.respond async (probably in another goroutine, in order to async \
	//   that also means if you want to respond in the same goroutine, consider use the sync interface)
	// failed to do so will result in EventBus bad performance (it will wait for resp timeout)
	// DO NOT throw out panic!!! Eat your own shit!!!
	Process(bus EventBusWithRespond, event Event)
}

type SyncVerticalInterface interface {
	abstractVerticalInterface
	// return response at the end of event handling
	// you are NOT supposed to call respond method
	// DO NOT throw out panic!!! Eat your own shit!!!
	Process(bus EventBus, event Event) (response interface{}, err error)
}

// As explained above, a sync vertical could seen as an async vertical that respond at the end of processing
type syncVerticalWrapper struct {
	SyncVerticalInterface
}

func (wrapper syncVerticalWrapper) Process(bus EventBusWithRespond, event Event) {
	resp, err := wrapper.SyncVerticalInterface.Process(bus, event)
	bus.Respond(event, &EventResponse{resp, err})
}
