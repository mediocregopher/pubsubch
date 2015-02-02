// A package to supplement the pubsub package found in the radix suite. This one
// has the ability to receive incomming publishes over a channel. This channel
// will be close()'d when the connection is closed.
//
// The methods on the PubSubCh object should only ever be used syncronously.
// However, the PublishCh, where publishes are pushed to, *should* be read from
// asyncronously. Any blocking on it may block the whole object.
//
// See the example for a good example of how to use this package
package pubsubch

import (
	"fmt"
	"time"

	"github.com/mediocregopher/manatcp"
	"github.com/mediocregopher/pubsubch/inner"
	"github.com/mediocregopher/radix.v2/redis"
)

type Publish struct {
	Message string
	Channel string
}

// Container for the channeled pubsub connection. All methods should be called
// syncronously, however the PublishCh can (and should) be read from
// asyncronously.
type PubSubCh struct {
	i    *inner.MClient
	conn *manatcp.Conn

	// Channel which publishes from channels this connection is subscribed to
	// will be pushed onto. Not reading from this will cause the connection
	// routine to be blocked. This is the only part of PubSubCh which can be
	// interacted with in a different go-routine from the rest. It will be
	// close()'d when the connection is closed
	PublishCh chan *Publish
}

const DefaultTimeout = 30 * time.Second

// Creates a new PubSubCh with the given timeout. That timeout will be used when
// reading and writing to the underlying connection
func DialTimeout(addr string, timeout time.Duration) (*PubSubCh, error) {
	mc := inner.MClient{
		ReadCountCh: make(chan int, 1),
	}
	conn, err := manatcp.DialTimeout(mc, addr, timeout)
	if err != nil {
		return nil, err
	}

	pubCh := make(chan *Publish)
	go func() {
		for m := range conn.PushCh {
			// IsPush already determined it's an array of at least size 3
			arr, _ := m.(*redis.Resp).Array()
			ch, err := arr[1].Str()
			if err != nil {
				continue
			}
			msg, err := arr[2].Str()
			if err != nil {
				continue
			}
			pubCh <- &Publish{msg, ch}
		}
		close(pubCh)
	}()

	return &PubSubCh{
		i:         &mc,
		conn:      conn,
		PublishCh: pubCh,
	}, nil
}

// Like DialTimeout, but using the DefaultTimeout
func Dial(addr string) (*PubSubCh, error) {
	return DialTimeout(addr, DefaultTimeout)
}

func (p *PubSubCh) subUnsubGen(cmd string, args ...string) (int64, error) {
	fullArgs := append(make([]string, 0, len(args)+1), cmd)
	fullArgs = append(fullArgs, args...)

	p.i.ReadCountCh <- len(args)
	r, err, _ := p.conn.Cmd(fullArgs)
	if err != nil {
		return 0, err
	}
	arr, err := r.(*redis.Resp).Array()
	if err != nil {
		return 0, err
	}
	if len(arr) < 3 {
		return 0, fmt.Errorf("Unknown return: %#v", arr)
	}
	return arr[2].Int64()
}

func (p *PubSubCh) Subscribe(channel ...string) (int64, error) {
	return p.subUnsubGen("SUBSCRIBE", channel...)
}

// You *must* provide at least one channel here, the empty argument form of the
// command does not work with this package
func (p *PubSubCh) Unsubscribe(channel ...string) (int64, error) {
	return p.subUnsubGen("UNSUBSCRIBE", channel...)
}

func (p *PubSubCh) PSubscribe(pattern ...string) (int64, error) {
	return p.subUnsubGen("PSUBSCRIBE", pattern...)
}

// You *must* provide at least one pattern here, the empty argument form of the
// command does not work with this package
func (p *PubSubCh) PUnsubscribe(pattern ...string) (int64, error) {
	return p.subUnsubGen("PUNSUBSCRIBE", pattern...)
}

func (p *PubSubCh) Close() error {
	return p.conn.Close()
}
