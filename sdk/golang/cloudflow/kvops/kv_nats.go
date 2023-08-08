package kvops

import "github.com/nats-io/nats.go"

type NatsKVOp struct {
	url      string
	chprefix string
	nc       *nats.Conn
	js       nats.JetStreamContext
	st       *nats.StreamInfo
	subs     map[string]*nats.Subscription
	pulls    map[string]*nats.Subscription
	csmr     map[string]*nats.ConsumerInfo
}
