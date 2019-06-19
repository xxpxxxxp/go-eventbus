package common

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	set "github.com/deckarep/golang-set"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/xxpxxxxp/go-eventbus"
)

type CreateConditionRequest struct {
	BasePath   string
	Barrier    string
	Conditions []string
}

type CompleteConditionRequest struct {
	BasePath  string
	Barrier   string
	Condition string
}

type PushTaskRequest struct {
	QueuePath string
	Task      string
	Data      string
}

type WatchPathRequest string

type ChildChangeType int

const (
	ChildCreate ChildChangeType = iota
	ChildDelete
)

type ChildChangeEvent struct {
	Path  string
	Child string
	Type  ChildChangeType
}

var (
	CreateConditionRequestType   = reflect.TypeOf(&CreateConditionRequest{})
	CompleteConditionRequestType = reflect.TypeOf(&CompleteConditionRequest{})
	PushTaskRequestType          = reflect.TypeOf(&PushTaskRequest{})
	WatchPathRequestType         = reflect.TypeOf(WatchPathRequest(""))
	ChildChangeEventType         = reflect.TypeOf(&ChildChangeEvent{})
)

func NewZookeeperVertical() (eventbus.AsyncVerticalInterface, error) {
	if zkConn == nil || !zkConn.connectedSemaphore.WaitFor(time.Duration(5)*time.Second) {
		return nil, errors.New("no valid zookeeper connection")
	}

	return &zookeeperVertical{zkConn, nil, nil}, nil
}

type zookeeperVertical struct {
	*sealedZkConn
	notify   chan string
	response chan error
}

func (zoo *zookeeperVertical) Name() string {
	return "ZookeeperVertical"
}

func (zoo *zookeeperVertical) Interests() []reflect.Type {
	return []reflect.Type{CreateConditionRequestType, CompleteConditionRequestType, PushTaskRequestType, WatchPathRequestType}
}

func (zoo *zookeeperVertical) OnAttached(bus eventbus.EventBus) {
}

func (zoo *zookeeperVertical) OnDetached(bus eventbus.EventBus) {
}

func (zoo *zookeeperVertical) Process(ctx context.Context, bus eventbus.EventBusWithRespond, event eventbus.Event) {
	switch event.GetType() {
	case CreateConditionRequestType:
		req := event.GetBody().(*CreateConditionRequest)

		barrierPath := fmt.Sprintf("%s/%s", req.BasePath, req.Barrier)
		if _, err := zoo.createZnodeRecursive(barrierPath); err != nil && err != zk.ErrNodeExists {
			bus.Respond(event, &eventbus.EventResponse{Err: err})
			return
		}

		fullPaths := make([]string, len(req.Conditions))
		for i, condition := range req.Conditions {
			fullPaths[i] = fmt.Sprintf("%s/%s", barrierPath, condition)
		}
		if err := zoo.bulkCreateZnodes(fullPaths, nil, int32(0), zk.WorldACL(zk.PermAll)); err != nil {
			bus.Respond(event, &eventbus.EventResponse{Err: err})
			return
		}

		bus.Respond(event, &eventbus.EventResponse{})
	case CompleteConditionRequestType:
		req := event.GetBody().(*CompleteConditionRequest)

		barrierPath := fmt.Sprintf("%s/%s", req.BasePath, req.Barrier)
		conditionPath := fmt.Sprintf("%s/%s", barrierPath, req.Condition)

		if err := zoo.removeZnode(conditionPath); err != nil {
			bus.Respond(event, &eventbus.EventResponse{Err: err})
			return
		}

		// check if barrier is empty
		if children, _, err := zoo.Children(barrierPath); err == nil && len(children) == 0 {
			// remove barrier
			if err := zoo.Delete(barrierPath, -1); err != nil && err != zk.ErrNoNode {
				bus.Respond(event, &eventbus.EventResponse{Err: err})
				return
			}
		}

		bus.Respond(event, &eventbus.EventResponse{})
	case PushTaskRequestType:
		req := event.GetBody().(*PushTaskRequest)

		if _, err := zoo.createZnodeRecursive(req.QueuePath); err != nil && err != zk.ErrNodeExists {
			bus.Respond(event, &eventbus.EventResponse{Err: err})
			return
		}

		taskPath := fmt.Sprintf("%s/%s", req.QueuePath, req.Task)
		// create sequence task. pop will follow id asc
		if _, err := zoo.Create(taskPath, []byte(req.Data), zk.FlagSequence, zk.WorldACL(zk.PermAll)); err != nil {
			bus.Respond(event, &eventbus.EventResponse{Err: err})
			return
		}

		bus.Respond(event, &eventbus.EventResponse{})
	case WatchPathRequestType:
		req := string(event.GetBody().(WatchPathRequest))
		// HEADS UP! Magic coming!
		if zoo.notify == nil {
			zoo.notify = make(chan string)
			zoo.response = make(chan error)
			go func() {
				type watchContext struct {
					notify   <-chan zk.Event
					children []string
					basePath string
				}

				watchNodes := make([]*watchContext, 0)

				for {
					cases := make([]reflect.SelectCase, len(watchNodes)+2) // nodes + ctx + notify
					cases[0] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())}
					cases[1] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(zoo.notify)}

					for i, watch := range watchNodes {
						cases[i+2] = reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(watch.notify)}
					}

					chosen, value, _ := reflect.Select(cases)
					switch chosen {
					case 0: // ctx cancelled
						// TODO: cancel all watchers
						return
					case 1: // new watch request
						watchPath := value.String()

						// make sure the path exist
						if _, err := zoo.createZnodeRecursive(watchPath); err != nil && err != zk.ErrNodeExists {
							zoo.response <- err
							return
						}

						if children, _, c, err := zoo.ChildrenW(watchPath); err == nil {
							watchNodes = append(watchNodes, &watchContext{c, children, watchPath})
							zoo.response <- nil
						} else {
							zoo.response <- err
						}
					default:
						idx := chosen - 2
						wc := watchNodes[idx]
						previous := wc.children
						var err error
						if wc.children, _, wc.notify, err = zoo.ChildrenW(wc.basePath); err == nil {
							// compare with previous
							local := set.NewThreadUnsafeSet()
							for _, v := range previous {
								local.Add(v)
							}

							remote := set.NewThreadUnsafeSet()
							for _, v := range wc.children {
								remote.Add(v)
							}

							// node delete = local - remote
							for _, v := range previous {
								if !remote.Contains(v) {
									bus.Request(&ChildChangeEvent{wc.basePath, v, ChildDelete}, -1)
								}
							}

							// node add = remote - local
							for _, v := range wc.children {
								if !local.Contains(v) {
									bus.Request(&ChildChangeEvent{wc.basePath, v, ChildCreate}, -1)
								}
							}
						} else {
							// remove it from watching list
							watchNodes = append(watchNodes[:idx], watchNodes[idx+1:]...)
						}
					}
				}
			}()
		}

		zoo.notify <- req
		bus.Respond(event, &eventbus.EventResponse{Err: <-zoo.response})
	}
}
