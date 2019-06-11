EventBus for Go
===============

`go-eventbus` is a library of publish/subscribe event bus for Golang, inspired by [EventBus]((https://github.com/greenrobot/EventBus))

`go-eventbus` targets on decouple. Go language already ensure a good performance.

# Different decouple tech stacks
* [Inverse of control (aka. Dependency injection)](https://en.wikipedia.org/wiki/Inversion_of_control)

* EventBus

Components doesn't rely on each other. They publish events to EventBus, and retrieve interested event from that too.

* Actor model

Component call others **by name**. Controller hold name <--> components mapping. Then it's possible to use different components in different env for **same name**

---
As you can see, `go-eventbus` is designed for event based system. Normally, for event based system, workflow is like:
1. Something happened: __event__ generated
2. Some component, like __A__ do something to respond the __event__ (eg. when user click a url, open a website tab)
3. After that, __A__ wait for another event to come

# EventBus could help you decouple components
Instead of calling: `http.post(xxx)`

Consider using EventBus: `bus.Request(HttpPostEvent).GetResult()`

`HttpPostEvent` is responded by another component. It could made a real HTTP call in PRD, or it could return directly by a mocked component.
All that depends on what components are attached to the bus.

---
# Performance
## Benchmark:
* 1 response to each event: 50000~100000 event dispatch per sec
* 5 responses to each event: 20000 event dispatch per sec
* It's not ideal, performance is still improving

# Examples

## TODO