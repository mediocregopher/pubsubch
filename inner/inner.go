// Package inner contains some methods needed to implement an interface for
// manatcp. It's not meant to be used by anyone using the pubsubch package
package inner

import (
	"bufio"

	"github.com/mediocregopher/radix.v2/redis"
)

// MClient implements the manatcp Client interface
type MClient struct {
	// This will be initialized with a buffer of 1. When writing a command this
	// will have a number written to it containing the number of messages which
	// should be read off the connection (equal to number of channels
	// subscribed/unsubscribed to in the command, each one gets a message)
	ReadCountCh chan int
}

func (mc MClient) getReadCount() int {
	select {
	case i := <-mc.ReadCountCh:
		return i
	default:
		return 0
	}
}

// Read implements the Read method for the manatcp Client interface
func (mc MClient) Read(r *bufio.Reader) (interface{}, error, bool) {

	rr := redis.NewRespReader(r)
	var m *redis.Resp
	for {
		m = rr.Read()
		if redis.IsTimeout(m) {
			continue
		} else if m.Err != nil {
			return nil, m.Err, true
		}

		// We subtract one from the start because we've already read in one
		// message
		for i := mc.getReadCount() - 1; i > 0; i-- {
			m = rr.Read()
			if m.Err != nil {
				// we basically have to close, otherwise we'd get out of sync
				return nil, m.Err, true
			}
		}
		break
	}
	return m, nil, false
}

// IsPush implements the IsPush method for the manatcp Client interface
func (mc MClient) IsPush(i interface{}) bool {
	arr, err := i.(*redis.Resp).Array()
	if err != nil {
		return false
	}
	if len(arr) < 3 {
		return false
	}
	m, err := arr[0].Str()
	if err != nil {
		return false
	}
	return m == "message"
}

// Write implements the Write method for the manatcp Client interface
func (mc MClient) Write(w *bufio.Writer, i interface{}) (error, bool) {
	if _, err := redis.NewResp(i).WriteTo(w); err != nil {
		return err, true
	}
	return nil, false
}
