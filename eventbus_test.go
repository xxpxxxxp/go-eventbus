package eventbus

import (
	"container/list"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// Unit Test =================================================
type attachFunc func(bus EventBus)
type processFunc func(bus EventBus, event Event) (response interface{}, err error)

type utVertical struct {
	types       []reflect.Type
	attachFunc  attachFunc
	detachFunc  attachFunc
	processFunc processFunc
}

func (v *utVertical) Name() string {
	return "UTVertical"
}

func (v *utVertical) Interests() []reflect.Type {
	return v.types
}

func (v *utVertical) OnAttached(bus EventBus) {
	if v.attachFunc != nil {
		v.attachFunc(bus)
	}
}

func (v *utVertical) OnDetached(bus EventBus) {
	if v.detachFunc != nil {
		v.detachFunc(bus)
	}
}

func (v *utVertical) Process(bus EventBus, event Event) (response interface{}, err error) {
	if v.processFunc != nil {
		return v.processFunc(bus, event)
	}

	return event.GetBody(), nil
}

func TestBasicUsage(t *testing.T) {
	attached := false
	detached := false

	bus := NewEventBus(100, &DoNothingLogger{})

	if _, err := bus.Request(0, -1).GetResult(); err != ErrorEventNotInterested {
		t.Error("unexpected vertical response")
	}

	bus.AttachSyncVertical(&utVertical{
		types:      []reflect.Type{IntType},
		attachFunc: func(bus EventBus) { attached = true },
		detachFunc: func(bus EventBus) { detached = true },
	})

	time.Sleep(time.Millisecond)
	if !attached {
		t.Error("vertical not correctly attached")
	}

	if rst, err := bus.Request(0, -1).GetResult(); err != nil {
		t.Error("incorrect dispatch")
	} else {
		if val, ok := rst.Front().Value.(*EventResponse); !ok || val.Response != 0 {
			t.Error("incorrect response")
		}
	}

	bus.Shutdown()

	if _, err := bus.Request(0, -1).GetResult(); err != ErrorEventBusShutdown {
		t.Error("unexpected vertical response")
	}

	time.Sleep(time.Duration(10) * time.Millisecond)

	if !detached {
		t.Error("vertical not correctly detached")
	}
}

func TestEventTimeout(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.AttachSyncVertical(&utVertical{
		types: []reflect.Type{IntType},
		processFunc: func(bus EventBus, event Event) (response interface{}, err error) {
			time.Sleep(time.Duration(150) * time.Millisecond)
			return event.GetBody(), nil
		},
	})

	time.Sleep(time.Millisecond)

	start := time.Now().Nanosecond() / 1e6
	// each request will take 150ms
	f1 := bus.Request(0, -1)
	f2 := bus.Request(0, 200)
	f3 := bus.Request(0, 500)

	if rst, err := f1.GetResult(); err != ErrorResponseTimeout || rst.Len() != 0 || (time.Now().Nanosecond()/1e6)-start > 105 {
		t.Error("event with default timeout should be timeout")
	}

	if rst, err := f2.GetResult(); err != ErrorResponseTimeout || rst.Len() != 0 || (time.Now().Nanosecond()/1e6)-start > 305 {
		t.Error("event with 200ms timeout should be timeout")
	}

	if rst, err := f3.GetResult(); err != nil || rst.Len() != 1 || (time.Now().Nanosecond()/1e6)-start > 460 {
		t.Error("event with 500ms should not timeout")
	}

	bus.Shutdown()
}

// attach should not disturb response mapping
func TestAttachInRuntime(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.AttachSyncVertical(&utVertical{
		types: []reflect.Type{IntType},
		processFunc: func(bus EventBus, event Event) (response interface{}, err error) {
			time.Sleep(time.Duration(50) * time.Millisecond)
			return event.GetBody(), nil
		},
	})

	time.Sleep(time.Millisecond)

	f := bus.Request(0, -1)
	bus.AttachSyncVertical(&utVertical{
		types: []reflect.Type{IntType},
		processFunc: func(bus EventBus, event Event) (response interface{}, err error) {
			return event.GetBody(), nil
		},
	})

	if rst, err := f.GetResult(); err != nil {
		t.Error("incorrect dispatch")
	} else {
		if rst.Len() != 1 {
			t.Error("attach in runtime should not disturb dispatched event")
		}
		if val, ok := rst.Front().Value.(*EventResponse); !ok || val.Response != 0 {
			t.Error("incorrect response")
		}
	}

	bus.Shutdown()
}

// attach should not disturb response mapping
func TestDetachInRuntime(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	handle := bus.AttachSyncVertical(&utVertical{
		types: []reflect.Type{IntType},
		processFunc: func(bus EventBus, event Event) (response interface{}, err error) {
			time.Sleep(time.Duration(50) * time.Millisecond)
			return event.GetBody(), nil
		},
	})

	time.Sleep(time.Millisecond)

	f := bus.Request(0, -1)
	bus.Detach(handle)

	start := time.Now().Nanosecond() / 1e6
	if rst, err := f.GetResult(); err != nil {
		t.Error("incorrect dispatch")
	} else {
		if rst.Len() != 1 {
			t.Error("detach in runtime should not disturb dispatched event")
		}
		if val, ok := rst.Front().Value.(*EventResponse); !ok || val.Err != ErrorVerticalDetached || (time.Now().Nanosecond()/1e6)-start > 1 {
			t.Error("detach in runtime should cancel response mapping immediately")
		}
	}

	bus.Shutdown()
}

func TestAttachAfterShutdown(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.Shutdown()

	if bus.AttachSyncVertical(&utVertical{}) != VerticalHandle(0) {
		t.Error("vertical attach after shutdown should be discarded")
	}
}

func TestDetachAfterShutdown(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	handle := bus.AttachSyncVertical(&utVertical{types: []reflect.Type{IntType}})

	// no panic detaching not exist vertical
	bus.Detach(VerticalHandle(0))
	bus.Shutdown()
	// no panic detach after EventBus shutdown
	bus.Detach(handle)
}

type spinLock uint32

func (sl *spinLock) Lock() {
	for !atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1) {
		runtime.Gosched()
	}
}

func (sl *spinLock) Unlock() {
	atomic.StoreUint32((*uint32)(sl), 0)
}

func (sl *spinLock) TryLock() bool {
	return atomic.CompareAndSwapUint32((*uint32)(sl), 0, 1)
}

func newSpinLock() *spinLock {
	var lock spinLock
	return &lock
}

type podType struct {
	value    interface{}
	realType reflect.Type
}

var typeMapping = map[int]*podType{
	0: {true, BoolType},
	1: {byte(0), ByteType},
	2: {uint(0), UIntType},
	3: {0, IntType},
	4: {float32(0.0), Float32Type},
	5: {float64(0.0), Float64Type},
	6: {"", StringType},
}

type chaosProcessFunc func(id int, bus EventBus, event Event) (response interface{}, err error)

type chaosVertical struct {
	id          int
	processFunc chaosProcessFunc
}

func (v *chaosVertical) Name() string {
	return fmt.Sprintf("ChaosVertical_%d", v.id)
}

func (v *chaosVertical) Interests() []reflect.Type {
	return []reflect.Type{typeMapping[v.id].realType}
}

func (v *chaosVertical) OnAttached(bus EventBus) {
	time.Sleep(time.Second) // wait for all verticals initialized
}

func (v *chaosVertical) OnDetached(bus EventBus) {
}

func (v *chaosVertical) Process(bus EventBus, event Event) (response interface{}, err error) {
	if v.processFunc != nil {
		return v.processFunc(v.id, bus, event)
	}

	return event.GetBody(), nil
}

func TestChaos(t *testing.T) {
	rand.Seed(time.Now().UnixNano())
	handles := make(map[int]*list.List)
	lock := newSpinLock()

	var processFunc chaosProcessFunc
	processFunc = func(id int, bus EventBus, event Event) (response interface{}, err error) {
		if lock.TryLock() {
			switch rand.Intn(10) {
			case 0: // attach a new vertical
				nid := rand.Intn(len(typeMapping))
				handle := bus.AttachSyncVertical(&chaosVertical{nid, processFunc})
				handles[nid].PushBack(handle)
			case 1: // detach existing vertical
				nid := rand.Intn(len(typeMapping))
				hs := handles[nid]
				if hs.Len() > 0 {
					bus.Detach(hs.Front().Value.(VerticalHandle))
					hs.Remove(hs.Front())
				}
			default: // make a request
				if id != 0 {
					nid := rand.Intn(id) // always request lower id to avoid circle dependency
					// either no response at all, or the #response = #handle
					if rst, err := bus.Request(typeMapping[nid], -1).GetResult(); err != ErrorEventNotInterested && (rst != nil && rst.Len() != handles[nid].Len()) {
						t.Error("should not have dispatch error")
					} else {
						for e := rst.Front(); e != nil; e = e.Next() {
							if resp := e.Value.(*EventResponse); resp.Err != nil {
								t.Error("should not have error response")
							}
						}
					}
				}
			}

			lock.Unlock()
		} else {
			if id == 0 || rand.Intn(2) == 0 {
				bus.Request(typeMapping[rand.Intn(len(typeMapping))].value, -1)
			} else {
				_, _ = bus.Request(typeMapping[rand.Intn(id)], -1).GetResult()
			}
		}

		return event.GetBody(), nil
	}

	bus := NewEventBus(1000, &DoNothingLogger{})
	for i := 0; i < len(typeMapping); i++ {
		l := list.New()
		c := 1 + rand.Intn(5)
		for j := 0; j < c; j++ {
			l.PushBack(bus.AttachSyncVertical(&chaosVertical{i, processFunc}))
		}
		handles[i] = l
	}

	for i := 0; i < 10000; i++ {
		bus.Request(typeMapping[rand.Intn(len(typeMapping))].value, -1)
	}

	time.Sleep(time.Second)
	bus.Shutdown()
}

// Benchmark =================================================
type benchmarkVertical struct {
}

func (v *benchmarkVertical) Name() string {
	return "BenchmarkVertical"
}

func (v *benchmarkVertical) Interests() []reflect.Type {
	return []reflect.Type{IntType}
}

func (v *benchmarkVertical) OnAttached(bus EventBus) {
}

func (v *benchmarkVertical) OnDetached(bus EventBus) {
}

func (v *benchmarkVertical) Process(bus EventBus, event Event) (response interface{}, err error) {
	return event.GetBody(), nil
}

func BenchmarkRawCallBase(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{})
	vertical := &benchmarkVertical{}

	for i := 0; i < b.N; i++ {
		_, _ = vertical.Process(bus, &eventImpl{event: &event{body: i}})
	}

	bus.Shutdown()
}

func BenchmarkEventBusUnblock(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.AttachSyncVertical(&benchmarkVertical{})

	for i := 0; i < b.N; i++ {
		bus.Request(i, -1)
	}

	bus.Shutdown()
}

func BenchmarkEventBusBlock(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{}) //&ConsoleLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)})
	bus.AttachSyncVertical(&benchmarkVertical{})

	for i := 0; i < b.N; i++ {
		_, _ = bus.Request(i, -1).GetResult()
	}

	bus.Shutdown()
}

func BenchmarkEventBusMultipleVerticalUnblock(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})

	for i := 0; i < b.N; i++ {
		bus.Request(i, -1)
	}

	bus.Shutdown()
}

func BenchmarkEventBusMultipleBlock(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{}) //&ConsoleLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})
	bus.AttachSyncVertical(&benchmarkVertical{})

	for i := 0; i < b.N; i++ {
		_, _ = bus.Request(i, -1).GetResult()
	}

	bus.Shutdown()
}
