package eventbus

import "reflect"

type Event interface {
	GetBody() interface{}
	GetType() reflect.Type
}

type EventResponse struct {
	Response interface{}
	Err      error
}

type event struct {
	id     uint64
	future *futureImpl
	body   interface{}
}

type eventImpl struct {
	*event
	to        VerticalHandle
	eventType reflect.Type
}

func (ev *eventImpl) GetBody() interface{} {
	return ev.body
}

func (ev *eventImpl) GetType() reflect.Type {
	return ev.eventType
}

type innerEventResponse struct {
	*EventResponse
	id   uint64
	from VerticalHandle
}
