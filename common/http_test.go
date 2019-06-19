package common

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/xxpxxxxp/go-eventbus"
)

func TestHttpGet(t *testing.T) {
	if os.Getenv("JOB_NAME") != "" {
		t.Skip("skipping test in CI")
	}

	bus := eventbus.NewEventBus(1000, &eventbus.DoNothingLogger{})
	bus.AttachAsyncVertical(NewHTTPVertical(10000))

	if rst, err := bus.Request(&HTTPGetRequest{
		Urls: []string{
			"http://www.google.com",
			"http://www.bing.com",
			"http://www.yahoo.com",
		},
	}, 10000).GetResult(); err != nil {
		fmt.Println(err)
		t.Error("HTTP call should be correctly dispatched")
	} else {
		response := rst.Front().Value.(*eventbus.EventResponse)
		if response.Err != nil {
			t.Error("HTTP call should be correctly returned")
		}
		if httpResps, ok := response.Response.([]*HTTPVerticalResponse); !ok {
			t.Error("HTTP call should have correct response")
		} else {
			for _, resp := range httpResps {
				if resp.Error != nil || resp.Response.StatusCode != 200 || len(resp.Response.Body) == 0 {
					t.Error("HTTP should have valid response")
				}
			}
		}
	}

	time.Sleep(time.Duration(5) * time.Second)
	bus.Shutdown()
}

func TestHttpPost(t *testing.T) {
	if os.Getenv("JOB_NAME") != "" {
		t.Skip("skipping test in CI")
	}

	bus := eventbus.NewEventBus(1000, &eventbus.DoNothingLogger{})
	bus.AttachAsyncVertical(NewHTTPVertical(10000))

	if rst, err := bus.Request(&HTTPPostRequest{
		HTTPGetRequest{
			Urls: []string{
				"http://httpbin.org/post",
				"http://httpbin.org/post",
				"http://httpbin.org/post",
			},
		},
		"text/plain",
		[]byte("body doesn't matters"),
	}, 10000).GetResult(); err != nil {
		fmt.Println(err)
		t.Error("HTTP call should be correctly dispatched")
	} else {
		response := rst.Front().Value.(*eventbus.EventResponse)
		if response.Err != nil {
			t.Error("HTTP call should be correctly returned")
		}
		if httpResps, ok := response.Response.([]*HTTPVerticalResponse); !ok {
			t.Error("HTTP call should have correct response")
		} else {
			for _, resp := range httpResps {
				if resp.Error != nil || resp.Response.StatusCode != 200 || len(resp.Response.Body) == 0 {
					t.Error("HTTP should have valid response")
				}
			}
		}
	}

	time.Sleep(time.Duration(5) * time.Second)
	bus.Shutdown()
}
