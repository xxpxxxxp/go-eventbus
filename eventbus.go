package eventbus

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"github.com/scryner/lfreequeue"
	"github.com/uber-go/atomic"
)

// valid if > 0
type VerticalHandle uint64

// public object should not trigger respond
type EventBus interface {
	AttachAsyncVertical(v AsyncVerticalInterface) VerticalHandle
	AttachSyncVertical(v SyncVerticalInterface) VerticalHandle
	Detach(handle VerticalHandle)
	Request(ev interface{}, timeoutInMillis int64) Future // any timeout <= 0 will use default
	Shutdown()
}

// EventBusWithRespond is left as public for testing purpose only!
type EventBusWithRespond interface {
	EventBus
	Respond(ev Event, resp *EventResponse)
}

var (
	PlaceHolder             = struct{}{}
	ErrorEventNotInterested = errors.New("event not interested")
	ErrorEventBusShutdown   = errors.New("EventBus is shutting down")
	ErrorResponseTimeout    = errors.New("response timeout")
	ErrorVerticalDetached   = errors.New("vertical detached")
	ErrorVerticalNotFound   = errors.New("vertical not found")
)

// Implementation =============================================================
// be aware that timeout is rough - it may not be accurate for each request, accuracy is subject to system (in common case, ms level)
func NewEventBus(responseTimeoutInMilliSec int64, logger Logger) EventBus {
	ctx, cancelFunc := context.WithCancel(context.Background())
	bus := &eventBusImpl{
		logger:                    logger,
		dispatch:                  make(map[reflect.Type]set.Set),
		handle:                    make(map[VerticalHandle]*vertical),
		verticalDetachGroup:       &sync.WaitGroup{},
		ctx:                       ctx,
		cancelFunc:                cancelFunc,
		eventID:                   atomic.NewUint64(0),
		responseTimeoutInMilliSec: responseTimeoutInMilliSec,
		eventQueue:                lfreequeue.NewQueue(),
		eventNotifyChan:           make(chan struct{}, 1),
		orderedTimeout:            rbt.NewWith(func(f1, f2 interface{}) int { return int(f1.(*futureImpl).timeout - f2.(*futureImpl).timeout) }),
		futureMap:                 make(map[uint64]*futureImpl),
	}

	go bus.loop()
	logger.Infof("EventBus up & running, response timeout: %d ms", responseTimeoutInMilliSec)

	return bus
}

// implementation is totally lock free, because all request is handled in 1 goroutine sequentially
type eventBusImpl struct {
	logger Logger

	dispatch            map[reflect.Type]set.Set     // type <--> vertical handles
	handle              map[VerticalHandle]*vertical // handle <--> vertical
	verticalDetachGroup *sync.WaitGroup

	ctx                       context.Context
	cancelFunc                context.CancelFunc
	eventID                   *atomic.Uint64
	responseTimeoutInMilliSec int64
	eventQueue                *lfreequeue.Queue
	eventNotifyChan           chan struct{}

	/* For each event sent to a vertical, we expect the vertical respond once and only once (more than 1 respond should be discarded)
	   The future that received all responses is completed
	   Or if it's not completed after timeout, it's timeout
	*/
	orderedTimeout *rbt.Tree              // futures ordered by timeout in red-black tree
	futureMap      map[uint64]*futureImpl // id <--> future
}

// type alias so that user are allowed to reuse those public type as event
type verticalAttachEvent AsyncVerticalInterface
type verticalDetachEvent VerticalHandle

func (bus *eventBusImpl) AttachAsyncVertical(v AsyncVerticalInterface) VerticalHandle {
	rst, err := bus.Request(verticalAttachEvent(v), -1).GetResult()

	if err == nil {
		bus.logger.Infof("vertical attached, hello %s", v.Name())
		return rst.Front().Value.(VerticalHandle)
	}

	if err == ErrorEventBusShutdown {
		bus.logger.Infof("EventBus is shutting down, vertical %s attachment discarded", v.Name())
	} else {
		bus.logger.Errorf("vertical %s failed to attach with err %v", v.Name(), err)
	}
	return VerticalHandle(0)
}

func (bus *eventBusImpl) AttachSyncVertical(v SyncVerticalInterface) VerticalHandle {
	return bus.AttachAsyncVertical(&syncVerticalWrapper{v})
}

func (bus *eventBusImpl) Detach(handle VerticalHandle) {
	rst, err := bus.Request(verticalDetachEvent(handle), -1).GetResult()

	if err == nil {
		bus.logger.Infof("vertical detached, bye-bye %s", rst.Front().Value.(string))
	} else {
		if err == ErrorEventBusShutdown {
			bus.logger.Infof("EventBus is shutting down, vertical %d detachment discarded", handle)
		} else if err == ErrorVerticalNotFound {
			bus.logger.Infof("vertical handle %d not found", handle)
		} else {
			bus.logger.Errorf("vertical %d failed to detach with err %v", handle, err)
		}
	}
}

func (bus *eventBusImpl) Request(ev interface{}, timeoutInMillis int64) Future {
	if bus.ctx.Err() == nil {
		eventID := bus.eventID.Add(1)
		timeout := time.Now().UnixNano() / int64(1e6)
		if timeoutInMillis > 0 {
			timeout += timeoutInMillis
		} else {
			timeout += bus.responseTimeoutInMilliSec
		}
		f := newFuture(eventID, timeout)
		bus.eventQueue.Enqueue(&event{eventID, f, ev})

		select {
		case bus.eventNotifyChan <- PlaceHolder:
		default:
		}

		return f
	}
	return &failedFuture{ErrorEventBusShutdown}
}

func (bus *eventBusImpl) Respond(ev Event, resp *EventResponse) {
	casted := ev.(*eventImpl)
	_ = bus.Request(&innerEventResponse{resp, casted.id, casted.to}, -1) // no wait
}

func (bus *eventBusImpl) Shutdown() {
	bus.logger.Infoln("EventBus is shutting down!!!")
	bus.cancelFunc() // stop serving request & response
}

// Private methods =============================================================
func (bus *eventBusImpl) loop() {
	for {
		if ev, ok := bus.eventQueue.Dequeue(); ok {
			if bus.ctx.Err() != nil {
				bus.cleanup()
				return // EventBus is shutting down
			}
			bus.process(ev.(*event)) // process the event we got
		} else { // no event available now
			var waitTime int64 = 24 * 3600 * 1000
			if !bus.orderedTimeout.Empty() {
				waitTime = bus.orderedTimeout.Left().Key.(*futureImpl).timeout - (time.Now().UnixNano() / int64(time.Millisecond))
				if waitTime < 0 {
					waitTime = 0
				}
			}
			select {
			case <-bus.ctx.Done():
				bus.cleanup()
				return // EventBus is shutting down
			case <-time.After(time.Duration(waitTime) * time.Millisecond):
				bus.timeoutFutures()
			case <-bus.eventNotifyChan: // new event add to queue, continue
			}
		}
	}
}

func (bus *eventBusImpl) process(ev *event) {
	switch typedEvent := ev.body.(type) {
	case verticalAttachEvent: // vertical attaching
		ev.future.result.PushBack(bus.attach(AsyncVerticalInterface(typedEvent)))
		ev.future.setComplete()
	case verticalDetachEvent: // vertical detaching
		if name, err := bus.detach(VerticalHandle(typedEvent)); err == nil {
			ev.future.result.PushBack(name)
		} else {
			ev.future.err = err
		}
		ev.future.setComplete()
	case *innerEventResponse: // response mapping
		bus.mapResponse(typedEvent)
	default: // event dispatching
		bus.dispatchEvent(ev)
	}
	bus.timeoutFutures()
}

func (bus *eventBusImpl) attach(v AsyncVerticalInterface) VerticalHandle {
	// take a unused handle
	handle := func() VerticalHandle {
		for {
			h := VerticalHandle(generateID())
			if _, ok := bus.handle[h]; !ok {
				return h
			}
		}
	}()

	ctx, cancelFunc := context.WithCancel(bus.ctx)
	impl := &vertical{
		AsyncVerticalInterface: v,
		logger:                 bus.logger,
		interests:              v.Interests(), // make sure the interests doesn't change after attach
		ctx:                    ctx,
		cancelFunc:             cancelFunc,
		exitGroup:              bus.verticalDetachGroup,
		eventQueue:             lfreequeue.NewQueue(),
		eventNotifyChan:        make(chan struct{}, 1),
		handle:                 handle,
		eventBus:               bus,
	}

	go impl.loop()

	bus.handle[handle] = impl
	for _, concern := range impl.interests {
		if _, ok := bus.dispatch[concern]; !ok {
			bus.dispatch[concern] = set.NewThreadUnsafeSet() // EventBus is lock free
		}
		bus.dispatch[concern].Add(handle)
	}

	return handle
}

func (bus *eventBusImpl) detach(handle VerticalHandle) (string, error) {
	if v, ok := bus.handle[handle]; ok {
		v.cancelFunc()
		// remove vertical from mapping
		delete(bus.handle, handle)
		for _, interest := range v.interests {
			handles := bus.dispatch[interest]
			handles.Remove(handle)
			if handles.Cardinality() == 0 {
				delete(bus.dispatch, interest)
			}
		}

		// release dependent futures
		resp := &EventResponse{nil, ErrorVerticalDetached}
		for eventID, f := range bus.futureMap {
			if bus.tryCompleteFuture(f, handle, resp) {
				bus.logger.Infof("detaching vertical %s, abandon response to event %d", v.Name(), eventID)
			}
		}

		return v.Name(), nil
	}

	return "", ErrorVerticalNotFound
}

func (bus *eventBusImpl) mapResponse(response *innerEventResponse) {
	var name string
	if v, ok := bus.handle[response.from]; ok {
		name = v.Name()
	} else {
		name = "(unknown detached vertical)"
	}

	// response mapping
	if f, ok := bus.futureMap[response.id]; ok {
		if bus.tryCompleteFuture(f, response.from, response.EventResponse) {
			bus.logger.Infof("got response for %d from vertical %s", response.id, name)
		} else {
			bus.logger.Errorf("vertical %s respond more than 1 for event %d, redundant response discarded", name, response.id)
		}
	} else {
		bus.logger.Errorf("response for %d from vertical %s doesn't matched any event (maybe it missed the train?)", response.id, name)
	}
}

func (bus *eventBusImpl) dispatchEvent(ev *event) {
	eventType := reflect.TypeOf(ev.body)
	if handles, ok := bus.dispatch[eventType]; ok {
		ev.future.verticalChecking = handles.Clone()
		bus.futureMap[ev.id] = ev.future
		bus.orderedTimeout.Put(ev.future, PlaceHolder)

		// notice interested verticals
		for handle := range handles.Iter() {
			v := bus.handle[handle.(VerticalHandle)]
			bus.logger.Infof("transmit event %d to vertical %s", ev.id, v.Name())
			v.eventQueue.Enqueue(&eventImpl{ev, v.handle, eventType})

			select {
			case v.eventNotifyChan <- PlaceHolder:
			default:
			}
		}
	} else {
		bus.logger.Infof("event %d not interested by any vertical", ev.id)
		ev.future.err = ErrorEventNotInterested
		ev.future.setComplete()
	}
}

func (bus *eventBusImpl) tryCompleteFuture(f *futureImpl, from VerticalHandle, response *EventResponse) bool {
	// response mapping
	if f.verticalChecking.Contains(from) { // 1 vertical is allowed only 1 response to a event
		f.verticalChecking.Remove(from)
		f.result.PushBack(response)

		if f.verticalChecking.Cardinality() == 0 { // all responses collected
			bus.completeFuture(f)
		}
		return true
	}
	return false
}

func (bus *eventBusImpl) completeFuture(f *futureImpl) {
	bus.logger.Infof("future %d fulfilled", f.eventID)
	f.setComplete()

	// as future is fulfilled, remove the correspond timeout from tracking list
	bus.orderedTimeout.Remove(f)
	delete(bus.futureMap, f.eventID)
}

func (bus *eventBusImpl) timeoutFutures() {
	// clean timeout futures
	now := time.Now().UnixNano() / int64(1e6)
	for {
		if element := bus.orderedTimeout.Left(); element != nil {
			// notice the future about timeout
			f := element.Key.(*futureImpl)
			if f.timeout > now {
				break
			}

			bus.logger.Infof("future of event %d timeout at %d", f.eventID, now)

			f.err = ErrorResponseTimeout
			bus.completeFuture(f)
		} else {
			break
		}
	}
}

func (bus *eventBusImpl) cleanup() {
	bus.logger.Infoln("EventBus exiting...")
	// unblock all waiting futures
	for _, future := range bus.futureMap {
		future.err = ErrorEventBusShutdown
		bus.completeFuture(future)
	}

	bus.logger.Infoln("waiting for verticals join")
	exit := make(chan struct{})
	// wait all verticals join
	// in the meantime, we should still serving request: but this time, just return EventBus shutdown response
	go func() {
		for {
			if ev, ok := bus.eventQueue.Dequeue(); ok {
				// unblock incoming futures
				f := ev.(*event).future
				f.err = ErrorEventBusShutdown
				f.setComplete()
			} else { // no event available now
				select {
				case <-bus.eventNotifyChan: // new event add to queue, continue
				case <-exit: // all verticals joined
					return
				}
			}
		}
	}()

	bus.verticalDetachGroup.Wait()
	exit <- PlaceHolder // blocked until goroutine above exit
	bus.logger.Infoln("all verticals joined, EventBus off")

	// unhold all resources
	bus.dispatch = nil
	bus.handle = nil
	bus.eventQueue = nil
	bus.eventNotifyChan = nil
	bus.orderedTimeout = nil
	bus.futureMap = nil
}
