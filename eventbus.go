package eventbus

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	set "github.com/deckarep/golang-set"
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
	Request(ev interface{}) Future
	Shutdown()
}

// EventBusWithRespond is left as public for testing purpose only!
type EventBusWithRespond interface {
	EventBus
	Respond(ev Event, resp *EventResponse)
}

var (
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
		responseOwnerCheck:        make(map[uint64]set.Set),
		orderedTimeout:            list.New(),
		reverseLookup:             make(map[uint64]*list.Element),
	}

	go bus.loop()
	logger.Infoln(fmt.Sprintf("EventBus up & running, response timeout: %d ms", responseTimeoutInMilliSec))

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
	responseOwnerCheck map[uint64]set.Set       // id <--> dispatching verticals
	orderedTimeout     *list.List               // future's timeout ordered asc
	reverseLookup      map[uint64]*list.Element // id <--> future element in orderedTimeout mapping
}

// type alias so that user are allowed to reuse those public type as event
type verticalAttachEvent AsyncVerticalInterface
type verticalDetachEvent VerticalHandle

func (bus *eventBusImpl) AttachAsyncVertical(v AsyncVerticalInterface) VerticalHandle {
	rst, err := bus.Request(verticalAttachEvent(v)).GetResult()

	if err == nil {
		bus.logger.Infoln(fmt.Sprintf("vertical attached, hello %s", v.Name()))
		return rst.Front().Value.(VerticalHandle)
	}

	if err == ErrorEventBusShutdown {
		bus.logger.Infoln(fmt.Sprintf("EventBus is shutting down, vertical %s attachment discarded", v.Name()))
	} else {
		bus.logger.Errorln(fmt.Sprintf("vertical %s failed to attach with err %v", v.Name(), err))
	}
	return VerticalHandle(0)
}

func (bus *eventBusImpl) AttachSyncVertical(v SyncVerticalInterface) VerticalHandle {
	return bus.AttachAsyncVertical(&syncVerticalWrapper{v})
}

func (bus *eventBusImpl) Detach(handle VerticalHandle) {
	rst, err := bus.Request(verticalDetachEvent(handle)).GetResult()

	if err == nil {
		bus.logger.Infoln(fmt.Sprintf("vertical detached, bye-bye %s", rst.Front().Value.(string)))
	} else {
		if err == ErrorEventBusShutdown {
			bus.logger.Infoln(fmt.Sprintf("EventBus is shutting down, vertical %d detachment discarded", handle))
		} else if err == ErrorVerticalNotFound {
			bus.logger.Infoln(fmt.Sprintf("vertical handle %d not found", handle))
		} else {
			bus.logger.Errorln(fmt.Sprintf("vertical %d failed to detach with err %v", handle, err))
		}
	}
}

func (bus *eventBusImpl) Request(ev interface{}) Future {
	select {
	case <-bus.ctx.Done():
		return &failedFuture{ErrorEventBusShutdown}
	default:
		eventID := bus.eventID.Add(1)
		f := newFuture(eventID, bus.responseTimeoutInMilliSec+(time.Now().UnixNano()/int64(1e6)))
		bus.eventQueue.Enqueue(&event{eventID, f, ev})

		select {
		case bus.eventNotifyChan <- struct{}{}:
		default:
		}

		return f
	}
}

func (bus *eventBusImpl) Respond(ev Event, resp *EventResponse) {
	casted := ev.(*eventImpl)
	_ = bus.Request(&innerEventResponse{resp, casted.id, casted.to}) // no wait
}

func (bus *eventBusImpl) Shutdown() {
	bus.logger.Infoln("EventBus is shutting down!!!")
	bus.cancelFunc() // stop serving request & response
}

// Private methods =============================================================
func (bus *eventBusImpl) loop() {
	for {
		if ev, ok := bus.eventQueue.Dequeue(); ok {
			select {
			case <-bus.ctx.Done():
				bus.cleanup()
				return // EventBus is shutting down
			default:
				bus.process(ev.(*event)) // process the event we got
			}
		} else { // no event available now
			var waitTime int64 = 24 * 3600 * 1000
			if bus.orderedTimeout.Len() != 0 {
				waitTime = bus.orderedTimeout.Front().Value.(*futureImpl).timeout - (time.Now().UnixNano() / int64(time.Millisecond))
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
		ev.future.SetComplete()
	case verticalDetachEvent: // vertical detaching
		if name, err := bus.detach(VerticalHandle(typedEvent)); err == nil {
			ev.future.result.PushBack(name)
		} else {
			ev.future.err = err
		}
		ev.future.SetComplete()
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
			bus.dispatch[concern] = set.NewSet()
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
		for eventID, handles := range bus.responseOwnerCheck {
			if handles.Contains(handle) {
				bus.logger.Infoln(fmt.Sprintf("detaching vertical %s, abandon response to event %d", v.Name(), eventID))
				handles.Remove(handle)
				f := bus.reverseLookup[eventID].Value.(*futureImpl)
				f.result.PushBack(&EventResponse{nil, ErrorVerticalDetached})

				if handles.Cardinality() == 0 { // all responses collected
					bus.completeFuture(f)
				}
			}
		}

		return v.Name(), nil
	}

	return "", ErrorVerticalNotFound
}

func (bus *eventBusImpl) mapResponse(response *innerEventResponse) {
	// response mapping
	from := bus.handle[response.from].Name()
	if handles, ok := bus.responseOwnerCheck[response.id]; ok {
		if handles.Contains(response.from) { // 1 vertical is allowed only 1 response to a event
			bus.logger.Infoln(fmt.Sprintf("got response for %d from vertical %s", response.id, from))
			handles.Remove(response.from)

			f := bus.reverseLookup[response.id].Value.(*futureImpl)
			f.result.PushBack(response.EventResponse)

			if handles.Cardinality() == 0 { // all responses collected
				bus.completeFuture(f)
			}
		} else {
			bus.logger.Errorln(fmt.Sprintf("vertical %s respond more than 1 for event %d, redundant response discarded", from, response.id))
		}
	} else {
		bus.logger.Errorln(fmt.Sprintf("response for %d from vertical %s doesn't matched any event (maybe it missed the train?)", response.id, from))
	}
}

func (bus *eventBusImpl) dispatchEvent(ev *event) {
	eventType := reflect.TypeOf(ev.body)
	if handles, ok := bus.dispatch[eventType]; ok {
		bus.responseOwnerCheck[ev.id] = handles.Clone()
		bus.reverseLookup[ev.id] = bus.orderedTimeout.PushBack(ev.future)

		// notice interested verticals
		for handle := range handles.Iter() {
			v := bus.handle[handle.(VerticalHandle)]
			bus.logger.Infoln(fmt.Sprintf("transmit event %d to vertical %s", ev.id, v.Name()))
			v.eventQueue.Enqueue(&eventImpl{ev, v.handle, eventType})

			select {
			case v.eventNotifyChan <- struct{}{}:
			default:
			}
		}
	} else {
		bus.logger.Infoln(fmt.Sprintf("event %d not interested by any vertical", ev.id))
		ev.future.err = ErrorEventNotInterested
		ev.future.SetComplete()
	}
}

func (bus *eventBusImpl) completeFuture(f *futureImpl) {
	bus.logger.Infoln(fmt.Sprintf("future %d fulfilled", f.eventID))
	f.SetComplete()

	delete(bus.responseOwnerCheck, f.eventID)
	// as future is fulfilled, remove the correspond timeout from tracking list
	if element, ok := bus.reverseLookup[f.eventID]; ok {
		bus.orderedTimeout.Remove(element)
		delete(bus.reverseLookup, f.eventID)
	}
}

func (bus *eventBusImpl) timeoutFutures() {
	// clean timeout futures
	now := time.Now().UnixNano() / int64(1e6)
	for {
		if element := bus.orderedTimeout.Front(); element != nil && element.Value.(*futureImpl).timeout <= now {
			// notice the future about timeout
			f := element.Value.(*futureImpl)
			bus.logger.Infoln(fmt.Sprintf("future of event %d timeout at %d", f.eventID, now))

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
	for _, element := range bus.reverseLookup {
		future := element.Value.(*futureImpl)
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
				f.SetComplete()
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
	exit <- struct{}{} // blocked until goroutine above exit
	bus.logger.Infoln("all verticals joined, EventBus off")

	// unhold all resources
	bus.dispatch = nil
	bus.handle = nil
	bus.eventQueue = nil
	bus.eventNotifyChan = nil
	bus.responseOwnerCheck = nil
	bus.orderedTimeout = nil
	bus.reverseLookup = nil
}
