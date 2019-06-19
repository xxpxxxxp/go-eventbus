package common

import (
	"container/list"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/xxpxxxxp/go-eventbus"
)

var err = func() error {
	if err := InitZk("localhost:2181"); err != nil {
		return err
	}

	if !zkConn.connectedSemaphore.WaitFor(time.Duration(1) * time.Second) {
		return errors.New("unable to connect to local zookeeper")
	}

	return nil
}()

func TestZkCondition(t *testing.T) {
	if err != nil {
		t.Skip("skipping test without local zookeeper connection")
	}

	bus := eventbus.NewEventBus(1000, &eventbus.DoNothingLogger{})

	var v eventbus.AsyncVerticalInterface
	var err error
	if v, err = NewZookeeperVertical(); err != nil {
		t.Error("cannot instantiate zookeeper vertical")
	}

	bus.AttachAsyncVertical(v)

	// clean workspace
	_ = zkConn.removeRecursiveZnode("/some")

	basePath := "/some/path/that/I/really/dont/care"
	barrier := "barrier"
	conditions := []string{"cn1", "cn2", "cn3", "cn4", "cn5"}

	if rst, err := bus.Request(&CreateConditionRequest{
		basePath,
		barrier,
		conditions,
	}, -1).GetResult(); err != nil {
		t.Error("Zookeeper create condition request should be correctly dispatched")
	} else {
		response := rst.Front().Value.(*eventbus.EventResponse)
		if response.Err != nil {
			fmt.Println(response.Err)
			t.Error("Zookeeper create condition request should be correctly returned")
		}
	}

	// check if conditions successfully created
	for _, cn := range conditions {
		if ok, _, err := zkConn.Exists(fmt.Sprintf("%s/%s/%s", basePath, barrier, cn)); err != nil || !ok {
			t.Error("Zookeeper create condition request should be correctly executed")
		}
	}

	// let's try remove a condition
	if rst, err := bus.Request(&CompleteConditionRequest{
		basePath,
		barrier,
		conditions[0],
	}, -1).GetResult(); err != nil {
		t.Error("Zookeeper complete condition request should be correctly dispatched")
	} else {
		response := rst.Front().Value.(*eventbus.EventResponse)
		if response.Err != nil {
			t.Error("Zookeeper complete condition request should be correctly returned")
		}
	}

	// check if condition successfully completed
	if ok, _, err := zkConn.Exists(fmt.Sprintf("%s/%s/%s", basePath, barrier, conditions[0])); err != nil || ok {
		t.Error("Zookeeper complete condition request should be correctly executed")
	}

	bus.Shutdown()
}

func TestZkPushTask(t *testing.T) {
	if err != nil {
		t.Skip("skipping test without local zookeeper connection")
	}

	bus := eventbus.NewEventBus(1000, &eventbus.DoNothingLogger{})

	var v eventbus.AsyncVerticalInterface
	var err error
	if v, err = NewZookeeperVertical(); err != nil {
		t.Error("cannot instantiate zookeeper vertical")
	}

	bus.AttachAsyncVertical(v)

	// clean workspace
	_ = zkConn.removeRecursiveZnode("/some")

	basePath := "/some/path/that/I/really/dont/care"
	barrier := "queue"
	data := "guess what? some data that I don't care neither"

	// let's try push a task to queue
	if rst, err := bus.Request(&PushTaskRequest{
		basePath,
		barrier,
		data,
	}, -1).GetResult(); err != nil {
		t.Error("Zookeeper push task request should be correctly dispatched")
	} else {
		response := rst.Front().Value.(*eventbus.EventResponse)
		if response.Err != nil {
			t.Error("Zookeeper push task request should be correctly returned")
		}
	}

	// check if task successfully created
	taskPath := fmt.Sprintf("%s/%s0000000000", basePath, barrier)
	if d, _, err := zkConn.Get(taskPath); err != nil || string(d) != data {
		t.Error("Zookeeper push task request should be correctly executed")
	}

	bus.Shutdown()
}

type consumeZkEventVertical struct {
	events *list.Element
	t      *testing.T
}

func (v *consumeZkEventVertical) Name() string {
	return "DummyVertical"
}
func (v *consumeZkEventVertical) Interests() []reflect.Type {
	return []reflect.Type{ChildChangeEventType}
}
func (v *consumeZkEventVertical) OnAttached(bus eventbus.EventBus) {

}
func (v *consumeZkEventVertical) OnDetached(bus eventbus.EventBus) {

}
func (v *consumeZkEventVertical) Process(bus eventbus.EventBus, event eventbus.Event) (response interface{}, err error) {
	// check event (cannot compare pointer, need to de-reference)
	if *(v.events.Value.(*ChildChangeEvent)) != *(event.GetBody().(*ChildChangeEvent)) {
		v.t.Errorf("expecte event %+v, actual got %+v", v.events.Value, event.GetBody())
	}
	v.events = v.events.Next()

	return nil, nil
}

func TestZkWatchPath(t *testing.T) {
	if err != nil {
		t.Skip("skipping test without local zookeeper connection")
	}

	basePaths := []string{
		"/some/path/that/I/really/dont/care1",
		"/some/path/that/I/really/dont/care2",
		"/some/path/that/I/really/dont/care3",
	}
	barriers := []string{"barrier1", "barrier2", "barrier3"}
	conditions := []string{"cn1", "cn2", "cn3", "cn4", "cn5"}

	bus := eventbus.NewEventBus(1000, &eventbus.DoNothingLogger{})

	var v eventbus.AsyncVerticalInterface
	var err error
	if v, err = NewZookeeperVertical(); err != nil {
		t.Error("cannot instantiate zookeeper vertical")
	}

	bus.AttachAsyncVertical(v)

	evs := list.New()
	for _, basePath := range basePaths {
		for _, barrier := range barriers {
			evs.PushBack(&ChildChangeEvent{basePath, barrier, ChildCreate})
		}
		for _, barrier := range barriers {
			evs.PushBack(&ChildChangeEvent{basePath, barrier, ChildDelete})
		}
	}
	bus.AttachSyncVertical(&consumeZkEventVertical{evs.Front(), t})

	// clean workspace
	_ = zkConn.removeRecursiveZnode("/some")

	// let's watch some roots
	for _, root := range basePaths {
		if rst, err := bus.Request(WatchPathRequest(root), -1).GetResult(); err != nil {
			t.Error("Zookeeper push task request should be correctly dispatched")
		} else {
			response := rst.Front().Value.(*eventbus.EventResponse)
			if response.Err != nil {
				t.Error("Zookeeper push task request should be correctly returned")
			}
		}
	}

	// create some noise
	for _, basePath := range basePaths {
		for _, barrier := range barriers {
			_, _ = bus.Request(&CreateConditionRequest{basePath, barrier, conditions}, -1).GetResult()
		}
		for _, barrier := range barriers {
			_ = zkConn.removeRecursiveZnode(fmt.Sprintf("%s/%s", basePath, barrier))
		}
	}

	bus.Shutdown()
}
