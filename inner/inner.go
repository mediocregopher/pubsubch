// This package contains some methods needed to implement an interface for
// manatcp. It's not meant to be used by anyone using the pubsubch package
package inner

import (
	"bufio"
	"github.com/fzzy/radix/redis/resp"
	"net"
)

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

func (mc MClient) Read(r *bufio.Reader) (interface{}, error, bool) {

	var m *resp.Message
	var err error
	for {
		m, err = resp.ReadMessage(r)
		if nerr, ok := err.(*net.OpError); ok && nerr.Timeout() {
			continue
		} else if err != nil {
			return nil, err, true
		}

		// We subtract one from the start because we've already read in one
		// message
		for i := mc.getReadCount() - 1; i > 0; i-- {
			m, err = resp.ReadMessage(r)
			if err != nil {
				// we basically have to close, otherwise we'd get out of sync
				return nil, err, true
			}
		}
		break
	}
	return m, nil, false
}

func (_ MClient) IsPush(i interface{}) bool {
	arr, err := i.(*resp.Message).Array()
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

func (_ MClient) Write(w *bufio.Writer, i interface{}) (error, bool) {
	if err := resp.WriteArbitraryAsFlattenedStrings(w, i); err != nil {
		return err, true
	}
	return nil, false
}
