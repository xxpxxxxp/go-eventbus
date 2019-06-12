package eventbus

import (
	"container/list"
	"context"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/scryner/lfreequeue"
)

type fakedEventBus struct {
	events    *list.List
	responses *list.List
}

func (bus *fakedEventBus) AttachAsyncVertical(v AsyncVerticalInterface) VerticalHandle {
	return VerticalHandle(0)
}

func (bus *fakedEventBus) AttachSyncVertical(v SyncVerticalInterface) VerticalHandle {
	return VerticalHandle(0)
}

func (bus *fakedEventBus) Detach(handle VerticalHandle) {
}

func (bus *fakedEventBus) Request(ev interface{}) Future {
	return &failedFuture{}
}

func (bus *fakedEventBus) Shutdown() {
}

func (bus *fakedEventBus) Respond(ev Event, resp *EventResponse) {
	bus.events.PushBack(ev)
	bus.responses.PushBack(resp)
}

type testAsyncVertical struct {
	attached bool
	detached bool
	events   *list.List
}

func (v *testAsyncVertical) Name() string {
	return "AsyncVertical"
}

func (v *testAsyncVertical) Interests() []reflect.Type {
	return []reflect.Type{}
}

func (v *testAsyncVertical) OnAttached(bus EventBus) {
	v.attached = true
}

func (v *testAsyncVertical) OnDetached(bus EventBus) {
	v.detached = true
}

func (v *testAsyncVertical) Process(bus EventBusWithRespond, event Event) {
	v.events.PushBack(event)
	bus.Respond(event, &EventResponse{event, nil})
}

func TestAsyncVertical(t *testing.T) {
	bus := &fakedEventBus{list.New(), list.New()}
	inner := &testAsyncVertical{false, false, list.New()}

	ctx, cancelFunc := context.WithCancel(context.Background())
	v := &vertical{
		AsyncVerticalInterface: inner,
		interests:              inner.Interests(),
		ctx:                    ctx,
		cancelFunc:             cancelFunc,
		exitGroup:              &sync.WaitGroup{},
		eventQueue:             lfreequeue.NewQueue(),
		eventNotifyChan:        make(chan struct{}, 1),
		eventBus:               bus,
	}

	go v.loop()

	time.Sleep(time.Duration(10) * time.Millisecond)
	if !inner.attached {
		t.Error("vertical should be attached")
	}

	for i := 0; i < 10; i++ {
		v.eventQueue.Enqueue(&eventImpl{event: &event{body: i}})
		select {
		case v.eventNotifyChan <- struct{}{}:
		default:
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
	// event / response should be in order
	for i := 0; i < 10; i++ {
		ev1 := inner.events.Front()
		ev2 := bus.events.Front()
		resp := bus.responses.Front()

		if ev1.Value.(Event).GetBody() != i || ev2.Value.(Event).GetBody() != i || resp.Value.(*EventResponse).Response.(Event).GetBody() != i {
			t.Error("Event / Response is either incorrect or not in order")
		}

		inner.events.Remove(ev1)
		bus.events.Remove(ev2)
		bus.responses.Remove(resp)
	}

	v.cancelFunc()

	// wait a moment for the vertical detach
	time.Sleep(time.Duration(10) * time.Millisecond)
	if !inner.detached {
		t.Error("vertical should be detached")
	}
}

type testSyncVertical struct {
	attached bool
	detached bool
	events   *list.List
}

func (v *testSyncVertical) Name() string {
	return "SyncVertical"
}

func (v *testSyncVertical) Interests() []reflect.Type {
	return []reflect.Type{}
}

func (v *testSyncVertical) OnAttached(bus EventBus) {
	v.attached = true
}

func (v *testSyncVertical) OnDetached(bus EventBus) {
	v.detached = true
}

func (v *testSyncVertical) Process(bus EventBus, event Event) (response interface{}, err error) {
	v.events.PushBack(event)
	return event, nil
}

func TestSyncVertical(t *testing.T) {
	bus := &fakedEventBus{list.New(), list.New()}
	inner := &testSyncVertical{false, false, list.New()}

	ctx, cancelFunc := context.WithCancel(context.Background())
	v := &vertical{
		AsyncVerticalInterface: &syncVerticalWrapper{inner},
		interests:              inner.Interests(),
		ctx:                    ctx,
		cancelFunc:             cancelFunc,
		exitGroup:              &sync.WaitGroup{},
		eventQueue:             lfreequeue.NewQueue(),
		eventNotifyChan:        make(chan struct{}, 1),
		eventBus:               bus,
	}

	go v.loop()

	time.Sleep(time.Duration(10) * time.Millisecond)
	if !inner.attached {
		t.Error("vertical should be attached")
	}

	for i := 0; i < 10; i++ {
		v.eventQueue.Enqueue(&eventImpl{event: &event{body: i}})
		select {
		case v.eventNotifyChan <- struct{}{}:
		default:
		}
	}

	time.Sleep(time.Duration(1000) * time.Millisecond)
	// event / response should be in order
	for i := 0; i < 10; i++ {
		ev1 := inner.events.Front()
		ev2 := bus.events.Front()
		resp := bus.responses.Front()

		if ev1.Value.(Event).GetBody() != i || ev2.Value.(Event).GetBody() != i || resp.Value.(*EventResponse).Response.(Event).GetBody() != i {
			t.Error("Event / Response is either incorrect or not in order")
		}

		inner.events.Remove(ev1)
		bus.events.Remove(ev2)
		bus.responses.Remove(resp)
	}

	v.cancelFunc()

	// wait a moment for the vertical detach
	time.Sleep(time.Duration(10) * time.Millisecond)
	if !inner.detached {
		t.Error("vertical should be detached")
	}
}
