package eventbus

import (
	"math/rand"
	"time"
)

var random = rand.New(rand.NewSource(time.Now().UnixNano()))

func generateId() uint64 {
	return (uint64)((time.Now().UnixNano()/1000000)<<(63-41) | random.Int63n(1<<(63-41)))
}
