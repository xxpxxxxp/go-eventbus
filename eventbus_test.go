package eventbus

import (
	"reflect"
	"testing"
)

// Unit Test =================================================
type utVertical struct {
}

func (v *utVertical) Name() string {
	return "UTVertical"
}

func (v *utVertical) Interests() []reflect.Type {
	return []reflect.Type{IntType}
}

func (v *utVertical) OnAttached(bus EventBus) {
}

func (v *utVertical) OnDetached(bus EventBus) {
}

func (v *utVertical) Process(bus EventBus, event Event) (response interface{}, err error) {
	return event.GetBody(), nil
}

func TestBasicUsage(t *testing.T) {
	bus := NewEventBus(100, &DoNothingLogger{})
	bus.AttachSyncVertical(&utVertical{})

	if rst, err := bus.Request(0).GetResult(); err != nil {
		t.Error("Incorrect dispatch")
	} else {
		if val, ok := rst.Front().Value.(*EventResponse); !ok || val.Response != 0 {
			t.Error("Incorrect response")
		}
	}

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
		bus.Request(i)
	}

	bus.Shutdown()
}

func BenchmarkEventBusBlock(b *testing.B) {
	bus := NewEventBus(100, &DoNothingLogger{}) //&ConsoleLogger{log.New(os.Stdout, "", log.LstdFlags|log.Lshortfile)})
	bus.AttachSyncVertical(&benchmarkVertical{})

	for i := 0; i < b.N; i++ {
		_, _ = bus.Request(i).GetResult()
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
		bus.Request(i)
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
		_, _ = bus.Request(i).GetResult()
	}

	bus.Shutdown()
}
