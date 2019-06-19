package common

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"net/http"
	"reflect"
	"sync"
	"time"

	"github.com/xxpxxxxp/go-eventbus"
)

type HTTPGetRequest struct {
	Urls            []string
	Header          map[string][]string
	TimeoutInMillis int // set to 0 for default timeout
}

type HTTPPostRequest struct {
	HTTPGetRequest
	ContentType string // if ContentType is also set in headers, the one in headers will take priority
	Body        []byte
}

type HTTPResponse struct {
	StatusCode int
	Body       string
}

type HTTPVerticalResponse struct {
	Response *HTTPResponse
	Error    error
}

var (
	HTTPGetType  = reflect.TypeOf(&HTTPGetRequest{})
	HTTPPostType = reflect.TypeOf(&HTTPPostRequest{})
)

func NewHTTPVertical(defaultTimeoutInMillis int) eventbus.AsyncVerticalInterface {
	if defaultTimeoutInMillis <= 0 {
		panic("HTTP timeout cannot be 0 or negative")
	}

	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 20 * time.Second}).DialContext}

	return &httpVertical{
		defaultTimeout: defaultTimeoutInMillis,
		client:         &http.Client{Transport: transport, Timeout: time.Duration(defaultTimeoutInMillis) * time.Millisecond},
		transport:      transport,
	}
}

type httpVertical struct {
	defaultTimeout int
	// per https://golang.org/pkg/net/http/#Client
	// we should reuse same client if possible
	client    *http.Client
	transport *http.Transport
}

func (v *httpVertical) Name() string {
	return "HttpVertical"
}

func (v *httpVertical) Interests() []reflect.Type {
	return []reflect.Type{HTTPGetType, HTTPPostType}
}

func (v *httpVertical) OnAttached(bus eventbus.EventBus) {
}

func (v *httpVertical) OnDetached(bus eventbus.EventBus) {
}

func (v *httpVertical) Process(ctx context.Context, bus eventbus.EventBusWithRespond, event eventbus.Event) {
	switch event.GetType() {
	case HTTPGetType:
		raw := event.GetBody().(*HTTPGetRequest)
		requests := make([]*http.Request, len(raw.Urls))

		for i, url := range raw.Urls {
			req, _ := http.NewRequest("GET", url, nil)
			requests[i] = req.WithContext(ctx)
		}

		go v.request(bus, event, requests, raw.Header, raw.TimeoutInMillis)
	case HTTPPostType:
		raw := event.GetBody().(*HTTPPostRequest)
		requests := make([]*http.Request, len(raw.Urls))

		for i, url := range raw.Urls {
			req, _ := http.NewRequest("POST", url, bytes.NewReader(raw.Body))
			req.Header.Add("Content-Type", raw.ContentType)
			requests[i] = req.WithContext(ctx)
		}

		go v.request(bus, event, requests, raw.Header, raw.TimeoutInMillis)
	}
}

func (v *httpVertical) request(bus eventbus.EventBusWithRespond, event eventbus.Event, requests []*http.Request, headers map[string][]string, timeout int) {
	client := v.client
	if timeout > 0 && timeout != v.defaultTimeout {
		// need to use a new client
		client = &http.Client{Transport: v.transport, Timeout: time.Duration(timeout) * time.Millisecond}
	}

	if headers != nil {
		for _, req := range requests {
			for key, values := range headers {
				for _, value := range values {
					req.Header.Add(key, value)
				}
			}
		}
	}

	responses := make([]*HTTPVerticalResponse, len(requests))

	barrier := &sync.WaitGroup{}
	barrier.Add(len(requests))
	for i, request := range requests {
		go func(idx int, req *http.Request) {
			resp, err := client.Do(req)
			if err != nil {
				responses[idx] = &HTTPVerticalResponse{nil, err}
			} else {
				if body, err := ioutil.ReadAll(resp.Body); err == nil {
					httpResp := &HTTPResponse{
						StatusCode: resp.StatusCode,
						Body:       string(body),
					}

					responses[idx] = &HTTPVerticalResponse{httpResp, nil}
				} else {
					responses[idx] = &HTTPVerticalResponse{nil, err}
				}

				_ = resp.Body.Close()
			}
			barrier.Done()
		}(i, request)
	}

	barrier.Wait()
	bus.Respond(event, &eventbus.EventResponse{Response: responses})
}
