package chops

import (
	cf "cloudflow/sdk/golang/cloudflow"
	"strings"

	"github.com/nats-io/nats.go"
)

type NatsChOp struct {
	url      string
	chprefix string
	nc       *nats.Conn
	js       nats.JetStreamContext
	st       *nats.StreamInfo
	subs     map[string]*nats.Subscription
}

func NewNatsChOp(nc_url string, stream_name string) *NatsChOp {
	nc, err := nats.Connect(nats.DefaultURL)
	cf.Assert(err == nil, "connet Nats error: %s", err)
	js, err := nc.JetStream()
	cf.Assert(err == nil, "create jetstream error: %s", err)
	st, err := js.AddStream(&nats.StreamConfig{
		Name:     stream_name,
		Subjects: []string{stream_name + ".>"},
	})

	cf.Assert(err == nil, "create js.stream(%s) fail: %s", stream_name, err)
	ops := NatsChOp{
		url:      nc_url,
		chprefix: stream_name,
		nc:       nc,
		js:       js,
		st:       st,
		subs:     map[string]*nats.Subscription{},
	}
	return &ops
}

func (nt *NatsChOp) Put(ch_name []string, value string) bool {
	chs := nt.toSubjects(ch_name)
	for _, ch := range chs {
		_, err := nt.js.Publish(ch, []byte(value))
		cf.Assert(err == nil, "publish message to %s fail:%s", ch, err)
	}
	return true
}

func (nt *NatsChOp) Watch(ch_name []string, fc func(worker string, subj string, data string) bool) []string {
	cnkey := []string{}
	for _, sb := range nt.toSubjects(ch_name) {
		cs_key := cf.AsMd5(sb)
		sub, err := nt.js.QueueSubscribe(sb, cs_key, func(m *nats.Msg) {
			fc(cs_key, m.Subject, string(m.Data))
			m.Ack()
		}, nats.AckExplicit())
		cf.Assert(err == nil, "create QueueSubscribe(subj: %s) fail: %s", sb, err)
		nt.subs[cs_key] = sub
		cnkey = append(cnkey, cs_key)
	}
	return cnkey
}

func (nt *NatsChOp) Stop(cnkey []string) bool {
	for _, key := range cnkey {
		worker, val := nt.subs[key]
		if val {
			continue
		}
		err := worker.Unsubscribe()
		cf.Assert(err == nil, "unsub[%s] fail: %s", worker.Subject, err)
		delete(nt.subs, key)
	}
	return true
}

func (nt *NatsChOp) Close() bool {
	nt.js.PurgeStream(nt.chprefix)
	nt.js.DeleteStream(nt.chprefix)
	nt = nil
	return true
}

func (nt *NatsChOp) toSubjects(ch_name []string) []string {
	return cf.AddPrefix(ch_name, nt.chprefix+".")
}

func askey(ch_name []string) string {
	return cf.AsMd5(strings.Join(ch_name, "."))
}
